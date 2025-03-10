"""
Glue Objects and the helpers to manage them
"""

import asyncio
import glob
import json
import logging
import os.path

import httpx
from pydantic import BaseModel, computed_field
from watchfiles import Change, awatch


class VO(BaseModel):
    serial: int
    name: str


class VOStore:
    def __init__(self, settings):
        self.ops_portal_url = settings.ops_portal_url
        self.ops_portal_token = settings.ops_portal_token
        self._vos = []
        self._update_period = 60 * 60 * 2  # Every 2 hours

    def update_vos(self):
        try:
            r = httpx.get(
                self.ops_portal_url,
                headers={
                    "accept": "application/json",
                    "X-API-Key": self.ops_portal_token,
                },
            )
            r.raise_for_status()
            vos = []
            for vo_info in r.json()["data"]:
                vo = VO(**vo_info)
                vos.append(vo)
            self._vos = vos
        except httpx.HTTPStatusError as e:
            logging.error(f"Unable to load VOs: {e}")
            self._vos = []

    def get_vos(self):
        if not self._vos:
            self.update_vos()
        return self._vos

    async def start(self):
        while True:
            self.update_vos()
            await asyncio.sleep(self._update_period)


class GlueImage(BaseModel):
    name: str
    image: dict


class GlueInstanceType(BaseModel):
    name: str
    instance_type: dict


class GlueShare(BaseModel):
    name: str
    vo: str
    share: dict
    images: list[GlueImage]
    instancetypes: list[GlueInstanceType]

    def image_list(self):
        return [
            dict(
                appdb_id=img.image.get("imageVAppCName", ""),
                id=img.image.get("ID", ""),
                mpuri=img.image.get("MarketPlaceURL", ""),
                name=img.image.get("imageVAppName", ""),
            )
            for img in self.images
        ]

    def get_project(self):
        return dict(id=self.share["ProjectID"], name=self.vo)


class GlueSite(BaseModel):
    name: str
    service: dict
    service_id: str
    manager: dict
    manager_id: str
    endpoint: dict
    endpoint_id: str
    shares: list[GlueShare]

    def supports_vo(self, vo_name):
        return any(share.vo == vo_name for share in self.shares)

    def vo_share(self, vo_name):
        for share in self.shares:
            if share.vo == vo_name:
                return share
        else:
            return None

    @computed_field
    def gocdb_id(self) -> str:
        return self.service["OtherInfo"]["gocdb_id"]

    def summary(self):
        return dict(
            id=self.gocdb_id, name=self.name, url=self.endpoint["URL"], state=""
        )


class SiteStore:
    def __init__(self, settings):
        try:
            # This file contains the result of the GraphQL query
            # {
            #  siteCloudComputingImages {
            #    items {
            #      marketPlaceURL
            #      imageVAppCName
            #      imageVAppName
            #    }
            #  }
            # }
            # and then cleaned up
            with open(settings.appdb_images_file) as f:
                self._image_info = json.loads(f.read())
        except OSError as e:
            logging.error(f"Not able to load image info: {e.strerror}")
            self._image_info = {}

    async def start(self):
        return

    def _appdb_image_data(self, image_url):
        return self._image_info.get(image_url, {})

    def create_site(self, info):
        svc = info["CloudComputingService"][0]
        # yet another incongruency here
        mgr = info["CloudComputingManager"]
        ept = info["CloudComputingEndpoint"][0]

        shares = []
        for share_info in info["Share"]:
            for policy in info["MappingPolicy"]:
                if policy["Associations"]["Share"] == share_info["ID"]:
                    vo_name = policy["Associations"]["PolicyUserDomain"]
                    break
            else:
                logging.warning("No VO Name!?")
                continue
            images = []
            for image_info in info["CloudComputingImage"]:
                if share_info["ID"] in image_info["Associations"]["Share"]:
                    image_info.update(
                        self._appdb_image_data(image_info["MarketPlaceURL"])
                    )
                    images.append(GlueImage(name=image_info["Name"], image=image_info))
            instances = []
            for instance_info in info["CloudComputingInstanceType"]:
                if share_info["ID"] in instance_info["Associations"]["Share"]:
                    # who does not love a long attribute name?
                    acc_id = instance_info["Associations"].get(
                        "CloudComputingInstanceTypeCloudComputingVirtualAccelerator"
                    )
                    if acc_id:
                        for acc in info["CloudComputingVirtualAccelerator"]:
                            if acc["ID"] == acc_id:
                                instance_info.update({"accelerator": acc})
                    instances.append(
                        GlueInstanceType(
                            name=instance_info["Name"], instance_type=instance_info
                        )
                    )
            share = GlueShare(
                name=share_info["Name"],
                share=share_info,
                vo=vo_name,
                images=images,
                instancetypes=instances,
            )
            shares.append(share)
        site = GlueSite(
            name=svc["Associations"]["AdminDomain"][0],
            service=svc,
            service_id=svc["ID"],
            manager=mgr,
            manager_id=mgr["ID"],
            endpoint=ept,
            endpoint_id=ept["ID"],
            shares=shares,
        )
        return site

    def _sites():
        return []

    def get_sites(self, vo_name=None):
        if vo_name:
            sites = filter(lambda s: s.supports_vo(vo_name), self._sites())
        else:
            sites = self._sites()
        return sites

    def get_site_by_goc_id(self, gocdb_id):
        for site in self.get_sites():
            if site.gocdb_id == gocdb_id:
                return site
        return None

    def get_site_by_name(self, name):
        for site in self.get_sites():
            if site.name == name:
                return site
        return None

    def get_site_summary(self, vo_name=None):
        if vo_name:
            sites = filter(lambda s: s.supports_vo(vo_name), self.get_sites())
        else:
            sites = self.get_sites()
        return (s.summary() for s in sites)


class FileSiteStore(SiteStore):
    """
    Loads Site information from a directory that's watched for changes
    """

    def __init__(self, settings):
        super().__init__(settings)
        self.cloud_info_dir = settings.cloud_info_dir
        self._sites_files = {}

    def _load_site_file(self, path):
        filename = os.path.basename(path)
        try:
            with open(path) as f:
                s = self.create_site(json.loads(f.read()))
                self._sites_files[filename] = s
        except Exception as e:
            logging.error(f"Unable to load site {path}: {e}")

    def _rm_site(self, path):
        filename = os.path.basename(path)
        try:
            del self._sites_files[filename]
        except KeyError:
            logging.info(f"Site file {path} was not loaded")

    def _sites(self):
        return self._sites_files.values()

    async def start(self):
        for json_file in glob.glob(os.path.join(self.cloud_info_dir, "*")):
            self._load_site_file(json_file)
        async for changes in awatch(self.cloud_info_dir):
            for chg in changes:
                if chg[0] == Change.deleted:
                    self._rm_site(chg[1])
                else:
                    self._load_site_file(chg[1])


class S3SiteStore(SiteStore):
    def __init__(self, settings):
        super().__init__(settings)
        self.s3_url = settings.s3_url
        self._sites_info = {}
        self._update_period = 60 * 10  # 10 minutes

    def _load_site(self, site):
        name = site["name"]
        if name in self._sites_info:
            if site["last_modified"] == self._sites_info[name]["last_modified"]:
                # same update, no need to reload
                logging.info(f"No update neeeded for {name}")
                return {name: self._sites_info[name]}
        try:
            r = httpx.get(
                os.path.join(self.s3_url, name),
                headers={
                    "accept": "application/json",
                },
            )
            r.raise_for_status()
            try:
                site.update({"info": self.create_site(r.json())})
            except Exception as e:
                logging.error(f"Unable to load site {name}: {e}")
                return {}
            logging.info(f"Loaded info from {name}")
            return {name: site}
        except httpx.HTTPStatusError as e:
            logging.error(f"Unable to load site information: {e}")

    def _update_sites(self):
        new_sites = {}
        try:
            r = httpx.get(
                self.s3_url,
                headers={
                    "accept": "application/json",
                },
            )
            r.raise_for_status()
            for site in r.json():
                logging.error(f'Update site {site["name"]}')
                new_sites.update(self._load_site(site))
        except httpx.HTTPStatusError as e:
            logging.error(f"Unable to load Sites: {e}")
        # change all at once
        self._sites_info = new_sites

    def _sites(self):
        return (site["info"] for site in self._sites_info.values())

    async def start(self):
        while True:
            self._update_sites()
            await asyncio.sleep(self._update_period)
