

def compare_vest_strategy(num_shares, vest_price, predicted_price, short_term_tax_bucket=0.35, long_term_tax_bucket=0.20):
    """
    Compare sell-to-cover vs borrow-cash for gain/loss,  sell the remaining after 1 year.
    1. sell-to-cover
    2. borrow cash for tax

    :param num_shares:
    :param vest_price:
    :param predicted_price:
    :param short_term_tax_bucket:
    :param long_term_tax_bucket:
    :return:
    """

    # taxable when vested
    unavoided_tax_when_vested = num_shares * vest_price * short_term_tax_bucket

    # 1. sell-to-cover
    num_shares_sold_to_cover_tax = num_shares*short_term_tax_bucket
    num_shares_remain = num_shares - num_shares_sold_to_cover_tax

    long_term_tax_approach_1 = num_shares_remain * (predicted_price - vest_price) * long_term_tax_bucket
    total_gain_1 = num_shares_remain * predicted_price
    total_gain_1 -= long_term_tax_approach_1 if long_term_tax_approach_1 > 0 else 0

    # 2. borrow-cash
    borrow_cash = unavoided_tax_when_vested
    long_term_tax_approach_2 = num_shares * (predicted_price - vest_price) * long_term_tax_bucket
    total_gain_2 = num_shares * predicted_price - borrow_cash
    total_gain_2 -= long_term_tax_approach_2 if long_term_tax_approach_2 > 0 else 0

    # delta
    delta_gain = total_gain_2 - total_gain_1

    print("====share_strategy  vest_price=${}, predicted_price=${}".format(vest_price, predicted_price))

    print("sell-to-cover short-term {} long-term {} borrowed-cash: {}  total-gain={}".format(unavoided_tax_when_vested,
                                                                                             long_term_tax_approach_1,
                                                                                             0,
                                                                                             total_gain_1)
          )

    print("borrow-cash   short-term {} long-term {} borrowed-cash: {}  total-gain={}".format(unavoided_tax_when_vested,
                                                                                             long_term_tax_approach_2,
                                                                                             borrow_cash,
                                                                                             total_gain_2)
          )
    print("(borrow-cash  - sell-to-cover) = ${} \n".format(delta_gain))


def test_share_strategy():
    num_shares = 100
    vest_price = 25
    short_term_tax_bucket=0.35
    long_term_tax_bucket=0.20

    # case 1, stable stock
    predicted_price = vest_price
    compare_vest_strategy(num_shares, vest_price, predicted_price, short_term_tax_bucket, long_term_tax_bucket)

    # case 2,  stock drop unexpectedly
    predicted_price = 15
    compare_vest_strategy(num_shares, vest_price, predicted_price, short_term_tax_bucket, long_term_tax_bucket)

    # case 3,  stock crashed
    predicted_price = 1
    compare_vest_strategy(num_shares, vest_price, predicted_price, short_term_tax_bucket, long_term_tax_bucket)

    # case 4,  stock up
    predicted_price = 35
    compare_vest_strategy(num_shares, vest_price, predicted_price, short_term_tax_bucket, long_term_tax_bucket)

    # case 5,  stock up&up!
    predicted_price = 100   # Luke's long-time prediction
    compare_vest_strategy(num_shares, vest_price, predicted_price, short_term_tax_bucket, long_term_tax_bucket)


if __name__ == "__main__":

    test_share_strategy()
