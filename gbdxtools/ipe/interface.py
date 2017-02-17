import uuid
import json
import types
import gbdxtools.ipe_image

class Op(object):
    def __init__(self, name):
        self._operator = name
        self._id = str(uuid.uuid4())
        self._edges = []
        self._nodes = []

    def __call__(self, *args, **kwargs):
        if all([isinstance(arg, gbdxtools.ipe_image.IpeImage) for arg in args]):
            self._ipe_image_call(*args, **kwargs)
        self._nodes = [{"id": self._id, "operator": self._operator, "parameters": {k:json.dumps(v) if not isinstance(v, types.StringTypes) else v for k,v in kwargs.iteritems()}}]
        for arg in args:
            self._nodes.extend(arg._nodes)
        self._edges = [{"id": "{}-{}".format(arg._id, self._id), "index": idx + 1, "source": arg._id, "destination": self._id}
                       for idx, arg in enumerate(args)]
        for arg in args:
            self._edges.extend(arg._edges)
        return self

    def _ipe_image_call(self, *args, **kwargs):
        pass


    def graph(self):
        return {
            "id": str(uuid.uuid4()),
            "edges": self._edges,
            "nodes": self._nodes
        }

class Ipe(object):
    def __getattr__(self, name):
        return Op(name=name)
