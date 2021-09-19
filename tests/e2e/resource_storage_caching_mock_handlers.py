import kopf

@kopf.on.create('customresourcedefinition')
def crd_create(spec, **kwargs):
    print("crd created")
    return {'message': 'created...'}

@kopf.on.create('customresourcedefinition')
def crd_update(spec, **kwargs):
    print("crd updated")
    return {'message': 'updated...'}

@kopf.on.create('kopfexamples')
def create_fn(spec, **kwargs):
    print("resource created")
    return {'message': 'created...'}

@kopf.on.update('kopfexamples')
def update_fn(spec, **kwargs):
    print("resource updated")
    return {'message': 'updated...'}

@kopf.on.delete('kopfexamples')
def delete_fn(spec, **kwargs):
    print("resource deleted")
    return {'message': 'deleted...'}