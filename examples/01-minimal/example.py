import kopf


@kopf.on.create('kopfexamples')
def create_fn(spec, **kwargs):
    return {'message': 'created...'}

@kopf.on.update('kopfexamples')
def create_fn(spec, **kwargs):
    return {'message': 'updated...'}
