package user_portrait.mode

/**
  * Created by ibf on 02/04.
  */
class User(
            userId: Long, userName: String, userSex: String,
            userBirthday: String, userAge: Long, constellation: String,
            bigAreaName: String, country: String, province: String,
            city: String, cityLevel: String, hexMail: String,
            opMail: String, hexPhone: String, forePhone: String,
            opPhone: String, addTime: String, loginIp: String,
            logSource: String, requestUserId: Long, totalMark: Long,
            usedMark: Long, levelName: String, blacklist: Long,
            isMarried: Long, education: String, profession: String
          ) extends java.io.Serializable {
  /**
    * 根据给定的分隔符格式化数据，形成字符串
    *
    * @param splitSymbol
    * @return
    */
  def formatted(splitSymbol: String): String = {
    s"""${userId}${splitSymbol}${userName}${splitSymbol}${userSex}${splitSymbol}${userBirthday}${splitSymbol}${userAge}${splitSymbol}${constellation}${splitSymbol}${bigAreaName}${splitSymbol}${country}${splitSymbol}${province}${splitSymbol}${city}${splitSymbol}${cityLevel}${splitSymbol}${hexMail}${splitSymbol}${opMail}${splitSymbol}${hexPhone}${splitSymbol}${forePhone}${splitSymbol}${opPhone}${splitSymbol}${addTime}${splitSymbol}${loginIp}${splitSymbol}${logSource}${splitSymbol}${requestUserId}${splitSymbol}${totalMark}${splitSymbol}${usedMark}${splitSymbol}${levelName}${splitSymbol}${blacklist}${splitSymbol}${isMarried}${splitSymbol}${education}${splitSymbol}${profession}"""
  }
}

object User {
  def apply(
             userId: Long, userName: String, userSex: String,
             userBirthday: String, userAge: Long, constellation: String,
             bigAreaName: String, country: String, province: String,
             city: String, cityLevel: String, hexMail: String,
             opMail: String, hexPhone: String, forePhone: String,
             opPhone: String, addTime: String, loginIp: String,
             logSource: String, requestUserId: Long, totalMark: Long,
             usedMark: Long, levelName: String, blacklist: Long,
             isMarried: Long, education: String, profession: String
           ): User = new User(userId, userName, userSex, userBirthday, userAge, constellation, bigAreaName, country, province, city, cityLevel, hexMail, opMail, hexPhone, forePhone, opPhone, addTime, loginIp, logSource, requestUserId, totalMark, usedMark, levelName, blacklist, isMarried, education, profession)
}
