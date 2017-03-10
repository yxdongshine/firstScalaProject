package user_portrait.mode


/**
 * Created by Administrator on 2017/3/9 0009.
 */
case class BdbCoupon(
                    userId: Long ,
                    couponId:Long ,
                    amt : Double ,
                    status : Int ,
                    endTime : String
                      ) {


  def formatted(splitSymbol: String): String = {
    s"""${userId}${splitSymbol}${couponId}${splitSymbol}${amt}${splitSymbol}${status}${splitSymbol}${endTime}"""
  }

}
