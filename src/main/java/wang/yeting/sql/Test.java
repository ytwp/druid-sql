package wang.yeting.sql;

import wang.yeting.sql.ast.SQLStatement;

/**
 * @author : weipeng
 * @since : 2021-12-01 6:32 下午
 */

public class Test {

    public static void main(String[] args) {
        new Test().testSqlParser();
    }

    public void testSqlParser() {
        String sql = "select id                                                                                      as id,\n" +
                "       order_code                                                                              as order_code,\n" +
                "       consignee                                                                               as consignee,\n" +
                "       postalcode                                                                              as postalcode,\n" +
                "       phone                                                                                   as phone,\n" +
                "       contact_address                                                                         as contact_address,\n" +
                "       remark                                                                                  as remark,\n" +
                "       receiving_address_id                                                                    as receiving_address_id,\n" +
                "       audit_time                                                                              as audit_datetime,\n" +
                "       substr(audit_time, 1, 10)                                                               as audit_date,\n" +
                "       order_time                                                                              as order_datetime,\n" +
                "       substr(order_time, 1, 10)                                                               as order_date,\n" +
                "       shipments_time                                                                          as shipments_datetime,\n" +
                "       substr(shipments_time, 1, 10)                                                           as shipments_date,\n" +
                "       receiving_time                                                                          as receiving_datetime,\n" +
                "       substr(receiving_time, 1, 10)                                                           as receiving_date,\n" +
                "       pay_time                                                                                as pay_datetime,\n" +
                "       substr(pay_time, 1, 10)                                                                 as pay_date,\n" +
                "       last_update_time                                                                        as last_update_datetime,\n" +
                "       substr(last_update_time, 1, 10)                                                         as last_update_date,\n" +
                "       bill_id                                                                                 as bill_id,\n" +
                "       user_id                                                                                 as user_id,\n" +
                "       coupon_code                                                                             as coupon_code,\n" +
                "       if(is_instalment = 1, 1, 0)                                                             as is_instalment,\n" +
                "       case\n" +
                "           when source < 6 then 1\n" +
                "           when source = 9 then 2\n" +
                "           else -1 end                                                                         as app_platform,\n" +
                "       case\n" +
                "           when source < 6 then '小象优品'\n" +
                "           when source = 9 then '新浪分期'\n" +
                "           else '其他' end                                                                       as app_plateform_desc,\n" +
                "       substr(order_code, 1, 2)                                                                as order_code_prefix,\n" +
                "       order_type                                                                              as prefix_order_type,\n" +
                "       find_business_code_desc('xxyp_instalment.ods_t_om_order', 'order_type',\n" +
                "                               order_type)                                                     as prefix_order_type_desc,\n" +
                "       source                                                                                  as order_source,\n" +
                "       shipments_status                                                                        as shipments_status,\n" +
                "       instalment_count                                                                        as instalment_count,\n" +
                "       good_source                                                                             as goods_source,\n" +
                "       find_business_code_desc('xxyp_instalment.ods_t_om_order', 'good_source', good_source)   as goods_source_desc,\n" +
                "       order_type                                                                              as order_type,\n" +
                "       first_flag                                                                              as first_flag,\n" +
                "       city_code                                                                               as city_code,\n" +
                "       has_buy_info                                                                            as has_buy_info,\n" +
                "       rebate_flag                                                                             as rebate_flag,\n" +
                "       is_bill                                                                                 as is_bill,\n" +
                "       pay_method                                                                              as pay_method,\n" +
                "       find_business_code_desc('xxyp_instalment.ods_t_om_order', 'pay_method', pay_method)     as pay_method_desc,\n" +
                "       if(pay_method in (4, 5, 7, 8), 1, 0)                                                    as is_card_pay,\n" +
                "       pay_status                                                                              as pay_status,\n" +
                "       find_business_code_desc('xxyp_instalment.ods_t_om_order', 'pay_status', pay_status)     as pay_status_desc,\n" +
                "       order_status                                                                            as order_status,\n" +
                "       find_business_code_desc('xxyp_instalment.ods_t_om_order', 'order_status', order_status) as order_status_desc,\n" +
                "       goods_total_money                                                                       as goods_total_amount,\n" +
                "       freight                                                                                 as freight,\n" +
                "       nvl(discount_coupon, 0)                                                                 as discount_coupon_amount,\n" +
                "       nvl(favorable_total_money, 0)                                                           as favorable_total_amount,\n" +
                "       deal_with_amount                                                                        as deal_with_amount,\n" +
                "       if(pay_method in ('4', '5', '7', '8'),\n" +
                "          cast(goods_total_money + nvl(favorable_total_money, 0) as bigint),\n" +
                "          cast(nvl(freight, 0) + nvl(discount_coupon, 0) + nvl(deal_with_amount, 0) +\n" +
                "               nvl(deal_with_total_amount, 0) as bigint))                                      as order_total_amount,\n" +
                "       instalment_amount                                                                       as instalment_amount,\n" +
                "       deal_with_total_amount                                                                  as order_taxes\n" +
                "from xxyp_instalment.ods_t_om_order;";
        String dbType = "hive";
        System.out.println("原始SQL 为 ： " + sql);
        String result = SQLUtils.format(sql, dbType);
        System.out.println(result);
        SQLStatement statement = SQLUtils.parseSingleStatement(sql, dbType);
        System.out.println(statement.toString());
    }
}
