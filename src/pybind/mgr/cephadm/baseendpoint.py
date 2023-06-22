try:
    import cherrypy
    from cherrypy._cpserver import Server
except ImportError:
    # to avoid sphinx build crash
    class Server:  # type: ignore
        pass

from cephadm.ssl_cert_utils import SSLCerts
from mgr_util import test_port_allocation, PortAlreadyInUse
from typing import TYPE_CHECKING, Any, NamedTuple, Callable, Optional, List, Dict
import secrets

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


class Route(NamedTuple):
    name: str
    route: str
    controller: Callable
    method: Optional[List[str]] = []


class BaseEndpoint:
    def __init__(self, mgr: "CephadmOrchestrator",
                 port: int = 0) -> None:
        self.root: Server
        self.mgr: "CephadmOrchestrator" = mgr
        self.ssl_certs = SSLCerts()
        self.ssl_cn: str = self.mgr.get_hostname()
        self.server_port: int = port
        self.server_addr: str = self.mgr.get_mgr_ip()
        self.KV_STORE_CERT: str = ''
        self.KV_STORE_KEY: str = ''
        self.auth: bool = self.mgr.secure_monitoring_stack
        self.enable_tls: bool = True
        self.validate_port: bool = False
        self.auth_user_kv_store: str = ''
        self.auth_pass_kv_store: str = ''
        self.routes: List[Route] = []
        self.script_name: str = '/'
        self.cp_dispatcher = cherrypy.dispatch.RoutesDispatcher()
        self.cp_config: Dict[str, Dict[str, Any]] = {
            '/': {
                'request.dispatch': self.cp_dispatcher,
            }
        }

    def validate_password(self, realm: str, username: str, password: str) -> bool:
        return (password == self.password and username == self.username)

    def configure_tls(self, server: Server) -> None:
        old_cert = self.mgr.get_store(self.KV_STORE_CERT)
        old_key = self.mgr.get_store(self.KV_STORE_KEY)
        if old_cert and old_key:
            self.ssl_certs.load_root_credentials(old_cert, old_key)
        else:
            self.ssl_certs.generate_root_cert(self.mgr.get_mgr_ip())
            self.mgr.set_store(self.KV_STORE_CERT, self.ssl_certs.get_root_cert())
            self.mgr.set_store(self.KV_STORE_KEY, self.ssl_certs.get_root_key())

        addr = self.mgr.get_mgr_ip()
        server.ssl_certificate, server.ssl_private_key = self.ssl_certs.generate_cert_files(self.ssl_cn, addr)

    def configure_routes(self, enable_auth: bool) -> None:

        for route in self.routes:
            self.cp_dispatcher.connect(**route._asdict())
        cherrypy.tree.mount(None, self.script_name, config=self.cp_config)

    def enable_auth(self) -> None:
        self.username = self.mgr.get_store(self.auth_user_kv_store)
        self.password = self.mgr.get_store(self.auth_pass_kv_store)
        if not self.password or not self.username:
            self.username = 'admin'  # TODO(redo): what should be the default username
            self.password = secrets.token_urlsafe(20)
            self.mgr.set_store(self.auth_user_kv_store, self.password)
            self.mgr.set_store(self.auth_pass_kv_store, self.username)

    def configure(self) -> None:
        self.root_server = self.root(self.mgr, self.server_port, self.server_addr)
        if self.auth:
            self.enable_auth()
        if self.enable_tls:
            self.configure_tls(self.root_server)
        # TODO: controller was self.root.POST
        self.configure_routes(enable_auth=self.auth)
        if self.validate_port:
            self.find_free_port()

    def find_free_port(self) -> None:
        max_port = self.server_port + 150
        while self.server_port <= max_port:
            try:
                test_port_allocation(self.server_addr, self.server_port)
                self.root_server.socket_port = self.server_port
                self.mgr.log.debug(f'Cephadm agent endpoint using {self.server_port}')
                return
            except PortAlreadyInUse:
                self.server_port += 1
        self.mgr.log.error(f'Cephadm agent could not find free port in range {max_port - 150}-{max_port} and failed to start')