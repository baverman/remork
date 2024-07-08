import pytest
from remork import router as router_, testinfra as remork_ti

from testinfra import backend, modules, get_host
from testinfra.backend import base


def make_router_pair():
    rl = router_.LocalRouter()
    rr = router_.Router()
    local_proto = router_.msg_proto_decode(rl.handle_msg)
    remote_proto = router_.msg_proto_decode(rr.handle_msg)
    router_.bg(router_.drain, rl.buffer, remote_proto)
    router_.bg(router_.drain, rr.buffer, local_proto)
    return rl, rr


class InProcessBackend(remork_ti.RemorkBackendMixin, base.BaseBackend):
    NAME = 'remork+inprocess'


backend.BACKENDS[InProcessBackend.NAME] = 'tests.conftest.InProcessBackend'


@pytest.fixture
def router():
    return make_router_pair()[0]


@pytest.fixture
def host(router):
    h = get_host('remork+inprocess://boo')
    h.backend.router = router
    return h
