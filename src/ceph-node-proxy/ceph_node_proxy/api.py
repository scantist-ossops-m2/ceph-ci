import cherrypy
from cherrypy._cpserver import Server
from threading import Thread, Event
from typing import Dict, Any, List
from ceph_node_proxy.util import Config, Logger, write_tmp_file
from ceph_node_proxy.basesystem import BaseSystem
from ceph_node_proxy.reporter import Reporter
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ceph_node_proxy.main import NodeProxy


@cherrypy.tools.auth_basic(on=True)
@cherrypy.tools.allow(methods=['PUT'])
@cherrypy.tools.json_out()
class Admin():
    def __init__(self, api: 'API') -> None:
        self.api = api

    @cherrypy.expose
    def start(self) -> Dict[str, str]:
        self.api.backend.start_client()
        # self.backend.start_update_loop()
        self.api.reporter.run()
        return {'ok': 'node-proxy daemon started'}

    @cherrypy.expose
    def reload(self) -> Dict[str, str]:
        self.api.config.reload()
        return {'ok': 'node-proxy config reloaded'}

    def _stop(self) -> None:
        self.api.backend.stop_update_loop()
        self.api.backend.client.logout()
        self.api.reporter.stop()

    @cherrypy.expose
    def stop(self) -> Dict[str, str]:
        self._stop()
        return {'ok': 'node-proxy daemon stopped'}

    @cherrypy.expose
    def shutdown(self) -> Dict[str, str]:
        self._stop()
        cherrypy.engine.exit()
        return {'ok': 'Server shutdown.'}

    @cherrypy.expose
    def flush(self) -> Dict[str, str]:
        self.api.backend.flush()
        return {'ok': 'node-proxy data flushed'}


class API(Server):
    def __init__(self,
                 backend: 'BaseSystem',
                 reporter: 'Reporter',
                 config: 'Config',
                 addr: str = '0.0.0.0',
                 port: int = 0) -> None:
        super().__init__()
        self.log = Logger(__name__)
        self.backend = backend
        self.reporter = reporter
        self.config = config
        self.socket_port = self.config.__dict__['server']['port'] if not port else port
        self.socket_host = addr
        self.subscribe()

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def memory(self) -> Dict[str, Any]:
        return {'memory': self.backend.get_memory()}

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def network(self) -> Dict[str, Any]:
        return {'network': self.backend.get_network()}

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def processors(self) -> Dict[str, Any]:
        return {'processors': self.backend.get_processors()}

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def storage(self) -> Dict[str, Any]:
        return {'storage': self.backend.get_storage()}

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def power(self) -> Dict[str, Any]:
        return {'power': self.backend.get_power()}

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def fans(self) -> Dict[str, Any]:
        return {'fans': self.backend.get_fans()}

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def firmwares(self) -> Dict[str, Any]:
        return {'firmwares': self.backend.get_firmwares()}

    def _cp_dispatch(self, vpath: List[str]) -> 'API':
        if vpath[0] == 'led':
            if cherrypy.request.method == 'GET':
                return self.get_led
            if cherrypy.request.method == 'PATCH':
                return self.set_led
        return self

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def get_led(self, **kw: Dict[str, Any]) -> Dict[str, Any]:
        return self.backend.get_led()

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['PATCH'])
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.auth_basic(on=True)
    def set_led(self, **kw: Dict[str, Any]) -> Dict[str, Any]:
        data = cherrypy.request.json
        rc = self.backend.set_led(data)

        if rc != 200:
            cherrypy.response.status = rc
            result = {'state': 'error: please, verify the data you sent.'}
        else:
            result = {'state': data['state'].lower()}
        return result

    def stop(self) -> None:
        self.unsubscribe()
        super().stop()


class NodeProxyApi(Thread):
    def __init__(self,
                 node_proxy: 'NodeProxy',
                 username: str,
                 password: str,
                 ssl_crt: str,
                 ssl_key: str) -> None:
        super().__init__()
        self.log = Logger(__name__)
        self.cp_shutdown_event = Event()
        self.node_proxy = node_proxy
        self.username = username
        self.password = password
        self.ssl_crt = ssl_crt
        self.ssl_key = ssl_key
        self.api = API(self.node_proxy.system,
                       self.node_proxy.reporter_agent,
                       self.node_proxy.config)

    def check_auth(self, realm: str, username: str, password: str) -> bool:
        return self.username == username and \
            self.password == password

    def shutdown(self) -> None:
        self.log.logger.info('Stopping node-proxy API...')
        self.cp_shutdown_event.set()

    def run(self) -> None:
        self.log.logger.info('node-proxy API configuration...')
        cherrypy.config.update({
            'environment': 'production',
            'engine.autoreload.on': False,
        })
        config = {'/': {
            'request.methods_with_bodies': ('POST', 'PUT', 'PATCH'),
            'tools.trailing_slash.on': False,
            'tools.auth_basic.realm': 'localhost',
            'tools.auth_basic.checkpassword': self.check_auth
        }}
        cherrypy.tree.mount(self.api, '/', config=config)
        # cherrypy.tree.mount(admin, '/admin', config=config)

        ssl_crt = write_tmp_file(self.ssl_crt,
                                 prefix_name='listener-crt-')
        ssl_key = write_tmp_file(self.ssl_key,
                                 prefix_name='listener-key-')

        self.api.ssl_certificate = ssl_crt.name
        self.api.ssl_private_key = ssl_key.name

        cherrypy.server.unsubscribe()
        cherrypy.engine.start()
        self.log.logger.info('node-proxy API started.')
        self.cp_shutdown_event.wait()
        self.cp_shutdown_event.clear()
        cherrypy.engine.stop()
        cherrypy.server.httpserver = None
        self.log.logger.info('node-proxy API shutdown.')
