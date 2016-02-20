import pyqb
from . import result
import uuid

class Vertex(object):
    def __init__(self, driver):
        self.driver = driver
    
    def create(self, typeClass, data = None):
        SQL = pyqb.Create('VERTEX').class_(typeClass).set(data)
        uid = uuid.uuid4()
        SQL.set('uuid', str(uid))
        SQL.set('suid', "%x%x" % uid.fields[0:2])
        SQL.set('type', 'vertex')
        response = self.driver.query(SQL.result())
        res = result.Result(response[0])
        return res
    
    def update(self, typeClass, criteria, data):
        SQL = pyqb.Update(typeClass).set(data).where(criteria).result()
        response = self.driver.query(SQL)
        return response[0]

    def search(self, typeClass, query):
        SQL = pyqb.Select().from_(typeClass).where('any().toLowerCase()', '%%%s%%' % query, operator='LIKE').result()
        response = self.driver.query(SQL, 2)
        res = result.ResultSet(response)
        return res
    
    def delete(self, typeClass, criteria):
        SQL = pyqb.Delete('VERTEX').class_(typeClass).where(criteria).result()
        response = self.driver.query(SQL)
        return response[0]

    def find(self, typeClass, criteria = None, depth = 0):
        SQL = pyqb.Select().from_(typeClass).where(criteria).result()
        response = self.driver.query(SQL, depth=depth)
        res = result.ResultSet(response)
        return res
