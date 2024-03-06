from fastapi import APIRouter

router = APIRouter(prefix="")


@router.get("/")
async def root():
    return {"message": "Hello World"}
