import pystreamMain as pm
publicationId = '6b1f5ea3-e89c-495d-8064-9de705f73ca5'  #publication id : what is being published
publisherKey = '40a2bedc-3797-40d4-a007-645b6b5bbed3'   #publisher key : who is publishing it
subscriptionId = '6b1f5ea3-e89c-495d-8064-9de705f73ca5'
subscriberKey = '40a2bedc-3797-40d4-a007-645b6b5bbed3'
configServer = 'localhost'
configDB = 'dataControl'

pm.publish_data(publicationId,publisherKey,configServer,configDB)
