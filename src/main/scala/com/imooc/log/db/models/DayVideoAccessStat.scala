package com.imooc.log.db.models

/**
 * 每天课程访问次数实体类/
 */
case class DayVideoAccessStat(day: String, cmsId: Long, times: Long)