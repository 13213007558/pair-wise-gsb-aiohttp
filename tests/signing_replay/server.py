"""aiohttp web app implementing the three retry/redirect scenarios."""

import asyncio

from aiohttp import web

from .audit import WireRecorder


def build_app(recorder: WireRecorder) -> web.Application:
    app = web.Application()

    async def warmup(request: web.Request) -> web.Response:
        # Fully answered request; its connection goes back into the pool and
        # is then closed below so the next signed request hits a stale socket.
        await request.read()
        return web.Response(text="warm")

    async def stale_then_close(request: web.Request) -> web.Response:
        # The first pooled-connection attempt dies before the handler runs;
        # the framework's automatic retry reaches this handler and is recorded.
        await recorder.record(request)
        response = web.Response(text="closing")
        await response.prepare(request)
        await response.write_eof()
        # Let the response leave the kernel before dropping the socket so the
        # client reliably pools the connection and only notices the reset on
        # its next attempt to reuse it.
        await asyncio.sleep(0.05)
        assert request.transport is not None
        request.transport.close()
        return response

    async def redirect_307(request: web.Request) -> web.Response:
        location = request.app.router["redirect_target"].url_for()
        return web.Response(
            status=307,
            headers={"Location": str(location)},
        )

    async def redirect_308(request: web.Request) -> web.Response:
        location = request.app.router["redirect_target"].url_for()
        return web.Response(
            status=308,
            headers={"Location": str(location)},
        )

    async def flaky_503(request: web.Request) -> web.Response:
        await recorder.record(request)
        if len(recorder.records) == 1:
            return web.Response(status=503, text="try again")
        return web.Response(text="recovered")

    app.router.add_post("/warmup", warmup)
    app.router.add_put("/stale", stale_then_close)
    app.router.add_put("/redirect307", redirect_307, name="redirect_307")
    app.router.add_put("/redirect308", redirect_308, name="redirect_308")
    app.router.add_put("/final", recorder.record, name="redirect_target")
    app.router.add_put("/flaky", flaky_503, name="flaky")
    return app
