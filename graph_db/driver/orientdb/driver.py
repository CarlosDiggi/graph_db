import requests
import forma

from ... import types
from . import exceptions

class DBDriver(types.BaseDBDriver):
    client = None
    _connected = False

    def __init__(self, autoConnect = True, settings={}):
        self._settings = settings
        user = self._settings['user']
        password = self._settings['password']
        self._auth = requests.auth.HTTPBasicAuth(user, password)
        if autoConnect:
            self.connect()

    def connect(self):
        if self._connected:
            return
        
        url = forma.rest(self._settings, 'connect')
        try:
            response = requests.get(url, auth=self._auth)
            if response.status_code == 401:
                raise exceptions.OrientDBConnectionError("Invalid Database 401 Connection")
            
            self._connected = True

        except requests.exceptions.RequestException as e:
            raise exceptions.OrientDBConnectionError("Invalid Database Connection (OrientDB may be down)")

    def query(self, sql, *args, **kwargs):
        if not self._connected:
            self.connect()
            
        depth = kwargs.get('depth', 0)
        url = forma.rest(self._settings, 'command','sql/-')

        try:
            response = requests.post(
                url, auth = self._auth, params = {'format': 'rid,class,fetchPlan:*:%d' % depth }, data = sql)
            return response.json().get('result')
        
        except requests.exceptions.RequestException as e:
            self._connected = False
            raise exceptions.OrientDBConnectionError("invalid connection (maybe down)")
        
        except ValueError as e:
            raise exceptions.OrientDBQueryError(response.text)
            
    def disconnect(self):
        if not self._connected:
            return
        
        url = forma.rest(self._settings, 'disconnect')
        
        try:
            response = requests.get(url, auth=self._auth)
            self._connected = False
        
        except requests.exceptions.RequestException as e:
            raise exceptions.OrientDBConnectionError("Couldn't Disconnect to OrientDB Server")
