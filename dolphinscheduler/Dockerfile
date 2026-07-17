FROM apache/dolphinscheduler-standalone-server:3.3.2

USER root
COPY plugins_config /opt/dolphinscheduler/conf/plugins_config
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io jq \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/dolphinscheduler/plugins/task-plugins \
    && curl -fL --retry 5 \
      https://repo1.maven.org/maven2/org/apache/dolphinscheduler/dolphinscheduler-task-shell/3.3.2/dolphinscheduler-task-shell-3.3.2-shade.jar \
      -o /opt/dolphinscheduler/plugins/task-plugins/dolphinscheduler-task-shell-3.3.2-shade.jar \
    && chown -R 1001:1001 /opt/dolphinscheduler/plugins
