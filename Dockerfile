FROM index.tenxcloud.com/docker_library/alpine
MAINTAINER yarntime@163.com

ADD remote_storage_adapter /usr/local/bin/remote_storage_adapter

EXPOSE 9201
ENTRYPOINT ["/usr/local/bin/remote_storage_adapter"]