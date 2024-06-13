package org.sunbird.obsrv.connector

object EventFixture {

  val INVALID_JSON = """{"name":"/v1/sys/health","context":{"trace_id":"7bba9f33312b3dbb8b2c2c62bb7abe2d""""
  val VALID_KAFKA_EVENT = """{"name":"/v1/sys/health","context":{"trace_id":"7bba9f33312b3dbb8b2c2c62bb7abe2d","span_id":"086e83747d0e381e"},"parent_id":"","start_time":"2021-10-22 16:04:01.209458162 +0000 UTC","end_time":"2021-10-22 16:04:01.209514132 +0000 UTC","status_code":"STATUS_CODE_OK","status_message":"","attributes":{"net.transport":"IP.TCP","net.peer.ip":"172.17.0.1","net.peer.port":"51820","net.host.ip":"10.177.2.152","net.host.port":"26040","http.method":"GET","http.target":"/v1/sys/health","http.server_name":"mortar-gateway","http.route":"/v1/sys/health","http.user_agent":"Consul Health Check","http.scheme":"http","http.host":"10.177.2.152:26040","http.flavor":"1.1"},"events":[{"name":"","message":"OK","timestamp":"2021-10-22 16:04:01.209512872 +0000 UTC"}]}"""

}
