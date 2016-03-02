class GraphDBException(Exception):
    pass
        
class BaseDBDriver():
    _connected = False
    _settings = {}
    def __init__(self, autoConnect = True, settings={}):
        self._settings.update(settings)
        if autoConnect:
            self.connect()
    def connect(self):
        raise NotImplementedError('Not Implemented Yet')
    def query(self, sql):
        raise NotImplementedError('Not Implemented Yet')
    def disconnect(self):
        raise NotImplementedError('Not Implemented Yet')


class BaseEdgeDriver(object):
    def __init__(self, driver):
        self.driver = driver
    def create(self, typeClass, From, to, data = None):
        raise NotImplementedError('Not Implemented Yet')
    
    def update(self, typeClass, criteria, data):
        raise NotImplementedError('Not Implemented Yet')

    def search(self, typeClass, query):
        raise NotImplementedError('Not Implemented Yet')
    
    def delete(self, typeClass, criteria):
        raise NotImplementedError('Not Implemented Yet')

    def find(self, typeClass, criteria = None, depth = 0):
        raise NotImplementedError('Not Implemented Yet')

class BaseVertexDriver(object):
    def __init__(self, driver):
        self.driver = driver
    
    def create(self, typeClass, data = None):
        raise NotImplementedError('Not Implemented Yet')
    
    def update(self, typeClass, criteria, data):
        raise NotImplementedError('Not Implemented Yet')

    def search(self, typeClass, query):
        raise NotImplementedError('Not Implemented Yet')
    
    def delete(self, typeClass, criteria):
        raise NotImplementedError('Not Implemented Yet')

    def find(self, typeClass, criteria = None, depth = 0):
        raise NotImplementedError('Not Implemented Yet')

class Map(dict):
    """
    Example:
    m = Map({'first_name': 'Eduardo'}, last_name='Pool', age=24, sports=['Soccer'])
    """
    def __init__(self, *args, **kwargs):
        super(Map, self).__init__(*args, **kwargs)
        for arg in args:
            if not(isinstance(arg, dict)):
                continue
            for k, v in arg.items():
                self[k] = v
        if not(kwargs):
            return
        for k, v in kwargs.items():
            self[k] = v

    def __getattr__(self, attr):
        return self.get(attr)

    def __setattr__(self, key, value):
        self.__setitem__(key, value)

    def __setitem__(self, key, value):
        super(Map, self).__setitem__(key, value)
        self.__dict__.update({key: value})

    def __delattr__(self, item):
        self.__delitem__(item)

    def __delitem__(self, key):
        super(Map, self).__delitem__(key)
        del self.__dict__[key]

class List(list):
    def size(self):
        return len(self)
    def take(self, count, offset=0):
        return self[count:count+offset]
    def first(self):
        return self[0]
    def last(self):
        return self[-1]
    def flatten(self):
        newItems = self.__class__()
        for item in self:
            if isinstance(item, List):
                newItems.extend(item.flatten())
            else:
                newItems.append(item)
        return newItems
    def filter(self, filteringLambda):
        newList = List()
        for item in self:
            filtered = None
            if isinstance(item, List):
                filtered = item.where(filteringLambda)
            elif filteringLambda(item):
                filtered = item
            if filtered is not None:
                newList.append(filtered)
        return newList

class Result(Map):
    def __init__(self, result, db):
        super(Result, self).__init__(result)
        self._db = db
        self._type = 'edge' if bool(r.get('in', False) and r.get('out', False)) else 'vertex'
    
    def __edge(self, edge):
        if isinstance(edge, str):
            return self.db.Edge.find(edge)
        else:
            return Result(edge, self._db)

    def __vertex(self, vertex):
        if isinstance(vertex, str):
            return self.db.Vertex.find(vertex)
        else:
            return Result(vertex, self._db)

    def edgeIn(self, label):
        if self._type != 'vertex':
            raise Exception('no edge from edge')
        edge = self['in_'+label]
        return self.__edge(edge)

    def edgeOut(self, label):
        if self._type != 'vertex':
            raise Exception('no edge from edge')
        edge = self['out_'+label]
        return self.__edge(edge)

    def vertexIn(self):
        if self._type == 'vertex':
            raise Exception('no vertex from vertex')

        return self.__vertex(self['in'])

    def vertexOut(self):
        if self._type == 'vertex':
            raise Exception('no vertex from vertex')

        return self.__vertex(self['out'])

class ResultSet(List):
    def __init__(self, resultset, db):
        super(ResultSet, self).__init__(resultset, db)
        self._db = db
        for i, target in enumerate(self):
            if isinstance(target, dict):
                self[i] = Result(target, self._db)
            elif isinstance(target, list):
                self[i] = ResultSet(target, self._db)
            else:
                raise Exception('extrange type %s -- %s' % (type(target).__name__, target) )

    def edgeIn(self, label='E'):
        nuevo = ResultSet(self)
        for i, target in enumerate(nuevo):
            self[i] = target.edgeIn(label)

    def edgeOut(self, label='E'):
        nuevo = ResultSet(self)
        for i, target in enumerate(nuevo):
            self[i] = target.edgeOut(label)

    def vertexIn(self, label='E'):
        nuevo = ResultSet(self)
        for i, target in enumerate(nuevo):
            self[i] = target.vertexIn(label)

    def vertexOut(self, label='E'):
        nuevo = ResultSet(self)
        for i, target in enumerate(nuevo):
            self[i] = target.vertexOut(label)
