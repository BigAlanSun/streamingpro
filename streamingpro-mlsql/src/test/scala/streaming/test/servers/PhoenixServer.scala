package streaming.test.servers

/**
  * @Author: Alan
  * @Time: 2019/1/3 12:18
  * @Description:
  */
class PhoenixServer(version: String) extends WowBaseTestServer{
  override def composeYaml: String =
    s"""
       |version: '2.1'
       |services:
       |  hbase:
       |    image: marcelocg/phoenix:${version}
       |    ports:
       |      - "2181:2181"
       |      - "8080:8080"
       |      - "8085:8085"
       |      - "9090:9090"
       |      - "9095:9095"
       |      - "16000:16000"
       |      - "16010:16010"
       |      - "16201:16201"
       |      - "16301:16301"
       |    hostname: phoenix-docker
    """.stripMargin

  override def waitToServiceReady: Boolean = ???
}
