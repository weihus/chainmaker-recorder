serverPort=9527

registerinfo:
	@curl localhost:${serverPort}/config/registerinfo

accessconfig:
	@curl localhost:${serverPort}/config/accessconfig

updateaccessconfig:
	@curl -XPUT \
	--data-binary @accessconfig.yml \
	localhost:${serverPort}/config/accessconfig

configvalue:
	@curl localhost:${serverPort}/config/configvalue

updateconfigvalue:
	@curl -XPUT \
	--data-binary @configvalue.yml \
	localhost:${serverPort}/config/configvalue
