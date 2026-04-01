import asyncio


async def handle_send_email(payload: dict) -> dict:
    # swap in real email logic (smtp, sendgrid, etc.)
    await asyncio.sleep(0.1)
    return {"sent_to": payload["to"]}


async def handle_process_data(payload: dict) -> dict:
    await asyncio.sleep(0.5)
    return {"processed": len(payload.get("items", []))}


HANDLERS = {
    "send_email": handle_send_email,
    "process_data": handle_process_data,
}
