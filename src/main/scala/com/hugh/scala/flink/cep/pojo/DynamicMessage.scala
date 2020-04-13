package com.hugh.scala.flink.cep.pojo

/**
 * @Author Fly.Hugh
 * @Date 2020/4/7 16:39
 * @Version 1.0
 **/
case class DynamicMessage(
                           imei:String,
                           id: String,   // id of project
                           lat: Double,
                           lng: Double,
                           time: String,
                           speed: Double
                          )
