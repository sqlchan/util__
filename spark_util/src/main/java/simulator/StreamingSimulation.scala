package simulator

import java.io.PrintWriter
import java.net.ServerSocket

import scala.io.Source

object StreamingSimulation {
  def index(length: Int ) ={
    import  java.util.Random
    val rdm = new Random()
    rdm.nextInt(length)
  }

  def main(args: Array[String]): Unit = {
    val filename ="E:\\util\\spark_util\\src\\main\\resources\\a.txt";
    val lines = Source.fromFile(filename).getLines().toList
    val filerow = lines.length

    val listener = new ServerSocket(8080)
    while (true){
      val socket = listener.accept()
      new Thread(){
        override def run={
          val out = new PrintWriter(socket.getOutputStream,true)
          while (true){
            Thread.sleep(1000)
            val content = lines(index(filerow))
            println(content)
            out.write(content+"\n")
            out.flush()
          }
          socket.close()
        }
      }.start()

    }
  }
}
