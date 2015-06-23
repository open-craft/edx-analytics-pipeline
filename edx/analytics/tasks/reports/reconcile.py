"""Perform reconciliation of transaction history against order history"""

from collections import namedtuple, defaultdict
from decimal import Decimal
import logging

import luigi
import luigi.date_interval

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionTask
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util.opaque_key_util import get_org_id_for_course

log = logging.getLogger(__name__)


ORDERITEM_FIELDS = [
    'order_processor',   # "shoppingcart" or "otto"
    'user_id',
    'order_id',
    'line_item_id',
    'line_item_product_id',  # for "shoppingcart", this is the kind of orderitem table.
    'line_item_price',
    'line_item_unit_price',
    'line_item_quantity',
    'product_class',  # e.g. seat, donation
    'course_id',  # Was called course_key
    'product_detail',  # contains course mode
    'username',
    'user_email',
    'date_placed',
    'iso_currency_code',
    'status',
    'refunded_amount',
    'refunded_quantity',
    'payment_ref_id',  # This is the value to compare with the transactions.
]

OrderItemRecord = namedtuple('OrderItemRecord', ORDERITEM_FIELDS)

# These are cybersource-specific at the moment, until generalized
# for Paypal, etc.
# Generalization will include:
#  time = timestamp the transaction was recorded (in addition to the date)
#  transaction_type: needs to be generalized (cybersource-specific terms now).
#  transaction_fee:  not reported in cybersource reports.
#
TRANSACTION_FIELDS = [
    'date',
    'payment_gateway_id',
    'payment_gateway_account_id',
    'payment_ref_id',
    'iso_currency_code',
    'amount',
    'transaction_fee',
    'transaction_type',
    'payment_method',
    'payment_method_type',
    'transaction_id',
]

TransactionRecord = namedtuple('TransactionRecord', TRANSACTION_FIELDS)

LOW_ORDER_ID_SHOPPINGCART_ORDERS = (
    '1556',
    '1564',
    '1794',
    '9280',
    '9918',
)


class ReconcileOrdersAndTransactionsDownstreamMixin(MapReduceJobTaskMixin):

    source = luigi.Parameter(
        is_list=True,
        default_from_config={'section': 'payment-reconciliation', 'name': 'source'}
    )

    # Create a dummy default for this parameter, since it is parsed by EventLogSelectionTask
    # but not actually used.
    interval = luigi.DateIntervalParameter(default=luigi.date_interval.Custom.parse("2014-01-01-2015-01-02"))

    pattern = luigi.Parameter(
        is_list=True,
        default_from_config={'section': 'payment-reconciliation', 'name': 'pattern'}
    )

    def extra_modules(self):
        """edx.analytics.tasks is required by all tasks that load this file."""
        import edx.analytics.tasks.mapreduce
        return [edx.analytics.tasks.mapreduce]


class ReconcileOrdersAndTransactionsTask(ReconcileOrdersAndTransactionsDownstreamMixin, MapReduceJobTask):
    """
    Compare orders and transactions.

    """

    output_root = luigi.Parameter()

    def requires(self):
        """Use EventLogSelectionTask to define inputs."""
        return EventLogSelectionTask(
            source=self.source,
            pattern=self.pattern,
            interval=self.interval,
        )

    def mapper(self, line):
        fields = line.split('\t')
        # If we put the "payment_ref_id" in the front of all these fields, or
        # at least always in the same index, then we wouldn't this
        # ugly heuristic here.  (It would only need to be in the
        # reducer. :)
        if len(fields) > 11:
            # assume it's an order
            key = fields[-1]
        else:
            # assume it's a transaction
            key = fields[3]
            # Edx-only: if the transaction was within a time period when
            # Otto was storing basket-id values instead of payment_ref_ids in
            # its transactions, then apply a heuristic to the transactions
            # from that period to convert them to a payment_ref_id that should
            # work in most cases.
            if fields[0] > '2015-05-01' and fields[0] < '2015-06-14':
                if len(key) <= 4 and key not in LOW_ORDER_ID_SHOPPINGCART_ORDERS:
                    key = 'EDX-{}'.format(int(key) + 100000)

        yield key, fields

    def _orderitem_is_professional_ed(self, orderitem):
        return orderitem.order_processor == 'shoppingcart' and orderitem.line_item_product_id in ['2', '3']

    def _orderitem_status_is_consistent(self, orderitem):
        return (
            (orderitem.status == 'purchased' and Decimal(orderitem.refunded_amount) == 0.0) or
            (orderitem.status == 'refunded' and Decimal(orderitem.refunded_amount) > 0.0)
        )

    def _add_orderitem_status_to_code(self, orderitem, code):
        if self._orderitem_status_is_consistent(orderitem):
            return code
        else:
            return "ERROR_WRONGSTATUS_{}".format(code)

    def _get_code_for_nonmatch(self, orderitem, trans_balance):
        code = "ERROR_{}_BALANCE_NOT_MATCHING".format(orderitem.status.upper())
        if trans_balance == Decimal(orderitem.line_item_price):
            # If these are equal, then the refunded_amount must be non-zero,
            # and not have a matching transaction.
            code = "{}_REFUND_MISSING".format(code)
        elif trans_balance == 0.0:
            code = "{}_WAS_REFUNDED".format(code)
        elif trans_balance == -1 * Decimal(orderitem.line_item_price):
            code = "{}_WAS_REFUNDED_TWICE".format(code)
        elif trans_balance == 2 * Decimal(orderitem.line_item_price):
            code = "{}_WAS_CHARGED_TWICE".format(code)
        elif trans_balance < Decimal(orderitem.line_item_price):
            code = "ERROR_BALANCE_NOT_MATCHING_PARTIAL_REFUND"
        elif trans_balance > Decimal(orderitem.line_item_price):
            code = "ERROR_BALANCE_NOT_MATCHING_EXTRA_CHARGE"
        code = self._add_orderitem_status_to_code(orderitem, code)
        return code

    def _extract_transactions(self, values):
        """
        Pulls orderitems and transactions out of input values iterable.

        Includes converting Hive-format nulls to appropriate string values.
        """
        orderitems = []
        transactions = []
        for value in values:
            if len(value) > 17:
                # convert refunded_amount:
                if value[16] == '\\N':
                    value[16] = '0.0'
                # same for 'refunded_quantity':
                if value[17] == '\\N':
                    value[17] = '0'
                # same for 'product_detail'
                if value[10] == '\\N':
                    value[10] = ''

                record = OrderItemRecord(*value)
                orderitems.append(record)
            else:
                if value[6] == '\\N':
                    value[6] = '0.0'
                transactions.append(TransactionRecord(*value))
        return orderitems, transactions

    def reducer(self, key, values):
        orderitems, transactions = self._extract_transactions(values)

        # Check to see that all orderitems belong to the same order.
        # If not, just set them aside for now.
        orderitem_partition = defaultdict(list)
        for orderitem in orderitems:
            orderitem_partition[orderitem.order_id] = orderitem
        if len(orderitem_partition) > 1:
            yield ("MULTIPLE_ORDERS", key, orderitems, transactions)

        # Calculate common values.
        trans_balance = Decimal(0.0)
        if len(transactions) > 0:
            trans_balance = sum([Decimal(transaction.amount) for transaction in transactions])
        order_balance = Decimal(0.0)
        if len(orderitems):
            order_balance = sum([Decimal(orderitem.line_item_price) - Decimal(orderitem.refunded_amount) for orderitem in orderitems])

        if len(transactions) == 0:
            # We have an orderitem with no transaction.  This happens
            # when an order is begun but the user changes their mind.

            # That said, there seem to be a goodly number of MITProfessionalX
            # entries that probably have transactions in a different account.

            # Also included are registrations that have no cost, so
            # having no transactions is actually a reasonable state.
            # These are dominated by DemoX registrations that
            # presumably demonstrate the process but have no cost.

            # And more are due to a difference in the timing of the
            # orders and the transaction extraction.  At present, the
            # orders are pulled at whatever time the task is run, and
            # they are dumped.  For transactions, the granularity is
            # daily: we only have up through yesterday's.  So there
            # may be orders from today that don't yet have
            # transactions downloaded.
            for orderitem in orderitems:
                code = "ERROR_NO_TRANSACTION"
                if self._orderitem_is_professional_ed(orderitem):
                    code = "NO_TRANS_PROFESSIONAL"
                elif Decimal(orderitem.line_item_unit_price) == 0.0:
                    code = "NO_TRANSACTION_NOCOST"
                code = self._add_orderitem_status_to_code(orderitem, code)
                yield ("TRANSACTION_TABLE", self.format_transaction_table_output(code, None, orderitem))

        elif len(orderitems) == 0:
            # Same thing if we have transactions with no orderitems.
            # This is likely when the transaction pull is newer than the order pull,
            # or if a shoppingcart basket was charged that was not marked as a purchased order.
            # In the latter case, if the charge was later refunded and the current balance
            # is zero, then no further action is needed.  Otherwise either the order needs
            # to be updated (to reflect that they did actually receive what they ordered),
            # or the balance should be refunded (because they never received what they were charged for).
            code = "NO_ORDER_ZERO_BALANCE" if trans_balance == 0 else "ERROR_NO_ORDER_NONZERO_BALANCE"
            for transaction in transactions:
                yield ("TRANSACTION_TABLE", self.format_transaction_table_output(code, transaction, None))
            return

        else:
            # This is the main case, where transactions are mapped to orderitems.
            order_status = "ORDER_BALANCED" if trans_balance == order_balance else "ERROR_ORDER_NOT_BALANCED"
            orderitem_transactions = self._map_transactions_to_orderitems(orderitems, transactions)
            
            for orderitem_dict in [orderitem_transactions]:
                for orderitem in orderitem_dict:
                    trans_list = orderitem_dict[orderitem]

                    # Get the status of the orderitem:
                    transaction_balance = Decimal(0.0)
                    for trans_entry in trans_list:
                        transaction, value, trans_status = trans_entry
                        transaction_balance += value
                    orderitem_status = self._get_orderitem_status(orderitem, transaction_balance)

                    for trans_entry in trans_list:
                        transaction, value, trans_status = trans_entry
                        code = "{}|{}|{}".format(order_status, orderitem_status, trans_status)
                        code = self._add_orderitem_status_to_code(orderitem, code)
                        yield (
                            "TRANSACTION_TABLE",
                            self.format_transaction_table_output(code, transaction, orderitem, value)
                        )

    def _get_orderitem_status(self, orderitem, trans_balance):
        orderitem_balance = Decimal(orderitem.line_item_price) - Decimal(orderitem.refunded_amount)
        code = "ERROR_{}_BALANCE_NOT_MATCHING".format(orderitem.status.upper())
        if trans_balance == orderitem_balance:
            code = "{}_BALANCE_MATCHING".format(orderitem.status.upper())
        elif trans_balance == Decimal(orderitem.line_item_price):
            # If these are equal, then the refunded_amount must be non-zero,
            # and not have a matching transaction.
            code = "{}_REFUND_MISSING".format(code)
        elif trans_balance == 0.0:
            code = "{}_WAS_REFUNDED".format(code)
        elif trans_balance == -1 * Decimal(orderitem.line_item_price):
            code = "{}_WAS_REFUNDED_TWICE".format(code)
        elif trans_balance == 2 * Decimal(orderitem.line_item_price):
            code = "{}_WAS_CHARGED_TWICE".format(code)
        elif trans_balance < Decimal(orderitem.line_item_price):
            code = "{}_PARTIAL_REFUND".format(code)
        elif trans_balance > Decimal(orderitem.line_item_price):
            code = "{}_EXTRA_CHARGE".format(code)
        code = self._add_orderitem_status_to_code(orderitem, code)
        return code

    def _map_purchases_to_orderitems(self, orderitems, purchases, orderitem_purchases):
        sorted_purchases = sorted(purchases, key=lambda x: x.date)
        order_cost = sum([Decimal(orderitem.line_item_price) for orderitem in orderitems])

        def get_purchase_status(orderitem, transaction_amount):
            """Return a status string depending on whether the item is already purchased or refunded."""
            orderitem_cost = Decimal(orderitem.line_item_price)
            if orderitem in orderitem_purchases:
                if transaction_amount == orderitem_cost:
                    status = 'PURCHASE_AGAIN'
                else:
                    status = 'PURCHASE_MISCHARGE_AGAIN'
            else:
                if transaction_amount == orderitem_cost:
                    status = 'PURCHASE_ONE'
                else:
                    status = 'PURCHASE_MISCHARGE'
            return status

        def get_orderitem_to_purchase(status, transaction):
            """Loop through orderitems, and refund the first one with a matching status."""
            transaction_amount = Decimal(transaction.amount)
            for orderitem in orderitems:
                if get_purchase_status(orderitem, transaction_amount) == status:
                    orderitem_purchases[orderitem].append((transaction, transaction_amount, status))
                    return orderitem
            return None

        for transaction in sorted_purchases:
            transaction_amount = Decimal(transaction.amount)
            # TODO: validation that transaction_amount > 0?

            if transaction_amount == order_cost:
                # transaction purchases all orderitems
                for orderitem in orderitems:
                    orderitem_cost =  Decimal(orderitem.line_item_price)
                    status = 'PURCHASE_AGAIN' if orderitem in orderitem_purchases else 'PURCHASE_ONE'
                    orderitem_purchases[orderitem].append((transaction, orderitem_cost, status))

            else:
                purchase_sequence = ['PURCHASE_ONE', 'PURCHASE_MISCHARGE', 'PURCHASE_AGAIN']
                found = any([get_orderitem_to_purchase(status, transaction) for status in purchase_sequence])

                if not found:
                    # We have a payment that doesn't align with one or all.
                    # It could be for two out of three, for example, but that
                    # should be rarer.  More likely is an overpayment or underpayment,
                    # that is a problem that needs to be flagged if it hasn't already
                    # been addressed.
                    status = 'PURCHASE_RANDOMLY'
                    orderitem_purchases[orderitems[0]].append((transaction, transaction_amount, status))

    def _map_transactions_to_orderitems(self, orderitems, transactions):

        orderitem_purchases = defaultdict(list)
        purchases = [transaction for transaction in transactions if Decimal(transaction.amount) >= 0]
        self._map_purchases_to_orderitems(orderitems, purchases, orderitem_purchases)

        orderitem_refunds = defaultdict(list)
        refunds = [transaction for transaction in transactions if Decimal(transaction.amount) < 0]
        sorted_refunds = sorted(refunds, key=lambda x: x.date)

        order_cost = sum([Decimal(orderitem.line_item_price) * -1 for orderitem in orderitems])

        def get_refund_status(orderitem):
            """Return a status string depending on whether the item is already purchased or refunded."""
            if orderitem in orderitem_purchases:
                if orderitem in orderitem_refunds:
                    status = 'REFUND_AGAIN' if orderitem.status == 'refunded' else 'REFUND_UNREFUNDED_AGAIN'
                else:
                    status = 'REFUND_ONE' if orderitem.status == 'refunded' else 'REFUND_UNREFUNDED'
            else:
                status = 'REFUND_UNPURCHASED'
            return status

        def get_orderitem_to_refund(status, transaction):
            """Loop through orderitems, and refund the first one with a matching status."""
            transaction_amount = Decimal(transaction.amount)
            for orderitem in orderitems:
                orderitem_cost = Decimal(orderitem.line_item_price) * -1
                if transaction_amount == orderitem_cost and get_refund_status(orderitem) == status:
                    orderitem_refunds[orderitem].append((transaction, orderitem_cost, status))
                    return orderitem
            return None

        for transaction in sorted_refunds:
            transaction_amount = Decimal(transaction.amount)
            if transaction_amount == order_cost:
                # transaction refunds all orderitems
                for orderitem in orderitems:
                    orderitem_cost = Decimal(orderitem.line_item_price) * -1
                    status = get_refund_status(orderitem)
                    orderitem_refunds[orderitem].append((transaction, orderitem_cost, status))
            else:
                # transaction_amount != order_cost overall, so try to find a particular
                # orderitem that has been paid for and should be refunded and has not yet been refunded.
                refund_sequence = ['REFUND_ONE', 'REFUND_UNREFUNDED', 'REFUND_AGAIN', 'REFUND_UNREFUNDED_AGAIN']
                found = any([get_orderitem_to_refund(status, transaction) for status in refund_sequence])

                if not found:
                    # What do we do?  Refund one thing at random...
                    status = 'REFUND_RANDOMLY'
                    orderitem_refunds[orderitems[0]].append((transaction, transaction_amount, status))

        # At this point, we should be done assigning all transactions to orderitems.
        # Collapse everything down into a single map.
        orderitem_transactions = defaultdict(list)
        for orderitem_dict in [orderitem_purchases, orderitem_refunds]:
            for orderitem in orderitem_dict:
                trans_list = orderitem_dict[orderitem]
                orderitem_transactions[orderitem].extend(trans_list)

        return orderitem_transactions

    def output(self):
        filename = u'reconcile_{reconcile_type}.tsv'.format(reconcile_type="all")
        output_path = url_path_join(self.output_root, filename)
        return get_target_from_url(output_path)

    def format_transaction_table_output(self, audit_code, transaction, orderitem, transaction_amount_per_item = None):
        transaction_fee_per_item = "0.0"
        if transaction_amount_per_item is None:
            transaction_amount_per_item = transaction.amount if transaction else ""

        if transaction:
            if transaction.amount == transaction_amount_per_item:
                transaction_fee_per_item = str(transaction.transaction_fee)
            else:
                proportion = Decimal(transaction_amount_per_item) / Decimal(transaction.amount)
                transaction_fee_per_item = str(Decimal(transaction.transaction_fee) * proportion)
        transaction_amount_per_item = str(transaction_amount_per_item)

        if orderitem:
            org_id = get_org_id_for_course(orderitem.course_id) or ""
        else:
            org_id = ""

        result = [
            audit_code,
            orderitem.payment_ref_id if orderitem else transaction.payment_ref_id,
            orderitem.order_id if orderitem else "",
            orderitem.date_placed if orderitem else "",
            # transaction information
            transaction.date if transaction else "",
            transaction.transaction_id if transaction else "",
            transaction.payment_gateway_id if transaction else "",
            transaction.payment_gateway_account_id if transaction else "",
            transaction.transaction_type if transaction else "",
            transaction.payment_method if transaction else "",
            transaction.amount if transaction else "",
            transaction.iso_currency_code if transaction else "",
            transaction.transaction_fee if transaction else "",
            # mapping information: part of transaction that applies to this orderitem
            transaction_amount_per_item,
            transaction_fee_per_item,
            # orderitem information
            orderitem.line_item_id if orderitem else "",
            orderitem.line_item_product_id if orderitem else "",
            orderitem.line_item_price if orderitem else "",
            orderitem.line_item_unit_price if orderitem else "",
            orderitem.line_item_quantity if orderitem else "",
            orderitem.refunded_amount if orderitem else "",
            orderitem.refunded_quantity if orderitem else "",
            orderitem.username if orderitem else "",
            orderitem.user_email if orderitem else "",
            orderitem.product_class if orderitem else "",
            orderitem.product_detail if orderitem else "",
            orderitem.course_id if orderitem else "",
            org_id,
            orderitem.order_processor if orderitem else "",
        ]
        return '\t'.join(result)


class ReconciliationOutputTask(ReconcileOrdersAndTransactionsDownstreamMixin, MultiOutputMapReduceJobTask):

    def requires(self):
        return ReconcileOrdersAndTransactionsTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.output_root,
            # overwrite=self.overwrite,
        )

    def mapper(self, line):
        """
        Groups inputs by reconciliation type, writes all records with the same type to the same output file.
        """
        reconcile_type, content = line.split('\t', 1)
        yield (reconcile_type), content

    def output_path_for_key(self, key):
        """
        Create a different output file based on the type (or category) of reconciliation.
        """
        reconcile_type = key.lower()
        filename = u'reconcile_{reconcile_type}.tsv'.format(reconcile_type=reconcile_type)
        return url_path_join(self.output_root, filename)

    def format_value(self, reconcile_type, value):
        """
        Transform a value into the right format for the given reconcile_type.
        """
        return value

    def multi_output_reducer(self, key, values, output_file):
        """
        Dump all the values with the same reconcile_type to the same file.

        """
        for value in values:
            formatted_value = self.format_value(key, value)
            output_file.write(formatted_value)
            output_file.write('\n')
