"""
SAIS — Hub WebSocket
Adicionar ao main.py:

    from app.core.websocket import hub, ws_router
    app.include_router(ws_router)
"""
import asyncio
import logging
from datetime import datetime
from typing import List
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

log = logging.getLogger("WS")

ws_router = APIRouter()


class WSHub:
    def __init__(self):
        self.connections: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.connections.append(ws)
        log.info(f"WS+ conectado | total: {len(self.connections)}")
        # Enviar estado inicial
        await ws.send_json({"tipo": "connected", "ts": datetime.now().isoformat(),
                            "clientes": len(self.connections)})

    def disconnect(self, ws: WebSocket):
        if ws in self.connections:
            self.connections.remove(ws)
        log.info(f"WS- desconectado | total: {len(self.connections)}")

    async def broadcast(self, message: dict):
        if not self.connections:
            return
        mortos = []
        for ws in self.connections:
            try:
                await ws.send_json(message)
            except Exception:
                mortos.append(ws)
        for ws in mortos:
            self.disconnect(ws)

    async def broadcast_evento(self, tipo: str, dados: dict):
        await self.broadcast({
            "tipo": tipo,
            "dados": dados,
            "ts": datetime.now().isoformat()
        })

    async def ping_loop(self, interval: int = 30):
        """Loop de ping para manter conexões vivas."""
        while True:
            await asyncio.sleep(interval)
            if self.connections:
                await self.broadcast({"tipo": "ping", "ts": datetime.now().isoformat()})


# Instância global — importar em outros módulos
hub = WSHub()


@ws_router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await hub.connect(websocket)
    try:
        while True:
            # Aguarda mensagem do cliente (pong, comandos, etc.)
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"tipo": "pong", "ts": datetime.now().isoformat()})
    except WebSocketDisconnect:
        hub.disconnect(websocket)
    except Exception as e:
        log.error(f"WS erro: {e}")
        hub.disconnect(websocket)
