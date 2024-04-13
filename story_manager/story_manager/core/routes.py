from fastapi import APIRouter

router = APIRouter(prefix="/stories", tags=["stories"])


@router.get("/stories")
async def get_stories():
    return {"message": "Hello, World!"}
