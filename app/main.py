"""
Appdb Information Sistem

A simple wrapper around the cloud-info jsons to deliver the information
needed by IM
"""

import asyncio
from contextlib import asynccontextmanager

from app.glue import S3SiteStore, VOStore
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pydantic_settings import BaseSettings


class Image(BaseModel):
    name: str
    appdb_id: str
    id: str
    mpuri: str


class Site(BaseModel):
    id: str
    name: str
    url: str
    state: str


class Project(BaseModel):
    id: str
    name: str


class Settings(BaseSettings):
    appdb_images_file: str = "data/appdb-images.json"
    ops_portal_url: str = "https://operations-portal.egi.eu/api/vo-list/json"
    ops_portal_token: str = ""
    cloud_info_dir: str = "cloud-info"
    s3_url: str = (
        "https://stratus-stor.ncg.ingrid.pt:8080/swift/v1/"
        "AUTH_bd5a81e1670b48f18af33b05512a9d77/cloud-info/"
    )


settings = Settings()
site_store = S3SiteStore(settings)
vo_store = VOStore(settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(vo_store.start())
    asyncio.create_task(site_store.start())
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/vos/")
def get_vos() -> list[str]:
    vos = sorted([vo.name for vo in vo_store.get_vos()])
    return vos


@app.get("/sites/")
def get_sites(vo_name: str = "") -> list[Site]:
    return [Site(**s.summary()) for s in site_store.get_sites(vo_name)]


@app.get("/site/")
def get_site(site_name: str = "") -> list[Site]:
    site = site_store.get_site_by_name(site_name)
    if not site:
        raise HTTPException(status_code=404, detail=f"Site {site_name} not found")
    return Site(**site.summary())


def _get_site(site_id: str, vo_name: str = ""):
    site = site_store.get_site_by_goc_id(site_id)
    if not site:
        raise HTTPException(status_code=404, detail=f"Site {site_id} not found")
    if vo_name and not site.supports_vo(vo_name):
        raise HTTPException(
            status_code=404, detail=f"VO {vo_name} not supported by Site {site_id}"
        )
    return site


@app.get("/site/{site_id}/")
def get_site_by_goc(site_id: str) -> Site:
    return Site(**_get_site(site_id).summary())


@app.get("/site/{site_id}/{vo_name}/images")
def get_images(site_id: str, vo_name: str) -> list[Image]:
    site = _get_site(site_id, vo_name)
    return [Image(**img) for img in site.vo_share(vo_name).image_list()]


@app.get("/site/{site_id}/projects")
def get_site_project_ids(site_id: str) -> list[Project]:
    site = _get_site(site_id)
    return [Project(**share.get_project()) for share in site.shares]


@app.get("/site/{site_id}/{vo_name}/project")
def get_project_id(site_id: str, vo_name: str) -> Project:
    site = _get_site(site_id, vo_name)
    return Project(**site.vo_share(vo_name).get_project())
