from fastapi import APIRouter
router = APIRouter()

@router.post("/enviar")
async def enviar_telegram():
    return {"status": "ok", "msg": "Em breve"}
