# Message types (matching Go client)
MESSAGE_TYPE_BATCH = 1
MESSAGE_TYPE_RESPONSE = 2

import logging


class DatasetType:
    MENU_ITEMS = 1
    STORES = 2
    TRANSACTION_ITEMS = 3
    TRANSACTIONS = 4
    USERS = 5

    Q1 = 10
    Q2 = 11
    Q3 = 12
    Q4 = 13


class Record:
    """Base interface for all record types"""

    def serialize(self):
        """Serialize record to string format"""
        raise NotImplementedError

    def get_type(self):
        """Get the dataset type of this record"""
        raise NotImplementedError


# Input dataset records
class MenuItemRecord(Record):
    """Menu item record: item_id, item_name, category, price, is_seasonal, available_from, available_to"""

    PARTS = 7

    def __init__(
        self,
        item_id,
        item_name,
        category,
        price,
        is_seasonal,
        available_from,
        available_to,
    ):
        self.item_id = item_id
        self.item_name = item_name
        self.category = category
        self.price = price
        self.is_seasonal = is_seasonal
        self.available_from = available_from
        self.available_to = available_to

    def serialize(self):
        return f"{self.item_id}|{self.item_name}|{self.category}|{self.price}|{self.is_seasonal}|{self.available_from}|{self.available_to}"

    def get_type(self):
        return DatasetType.MENU_ITEMS

    @classmethod
    def from_string(cls, data):
        parts = data.split("|")
        return cls.from_parts(parts)

    @classmethod
    def from_parts(cls, parts):
        if len(parts) < cls.PARTS:
            raise ValueError(
                f"Invalid MenuItemRecord format: expected 7 fields, got {len(parts)}"
            )
        return cls(*parts)

    @classmethod
    def get_field_count(cls):
        return 7


class StoreRecord(Record):
    """Store record: store_id, store_name, street, postal_code, city, state, latitude, longitude"""

    PARTS = 8

    def __init__(
        self,
        store_id,
        store_name,
        street,
        postal_code,
        city,
        state,
        latitude,
        longitude,
    ):
        self.store_id = store_id
        self.store_name = store_name
        self.street = street
        self.postal_code = postal_code
        self.city = city
        self.state = state
        self.latitude = latitude
        self.longitude = longitude

    def serialize(self):
        return f"{self.store_id}|{self.store_name}|{self.street}|{self.postal_code}|{self.city}|{self.state}|{self.latitude}|{self.longitude}"

    def get_type(self):
        return DatasetType.STORES

    @classmethod
    def from_string(cls, data):
        parts = data.split("|")
        return cls.from_parts(parts)

    @classmethod
    def from_parts(cls, parts):
        if len(parts) < cls.PARTS:
            raise ValueError(
                f"Invalid StoreRecord format: expected 8 fields, got {len(parts)}"
            )
        return cls(*parts)

    @classmethod
    def get_field_count(cls):
        return 8


class TransactionItemRecord(Record):
    """Transaction item record: transaction_id, item_id, quantity, unit_price, subtotal, created_at"""

    PARTS = 6

    def __init__(
        self, transaction_id, item_id, quantity, unit_price, subtotal, created_at
    ):
        self.transaction_id = transaction_id
        self.item_id = item_id
        self.quantity = quantity
        self.unit_price = unit_price
        self.subtotal = subtotal
        self.created_at = created_at

    def serialize(self):
        return f"{self.transaction_id}|{self.item_id}|{self.quantity}|{self.unit_price}|{self.subtotal}|{self.created_at}"

    def get_type(self):
        return DatasetType.TRANSACTION_ITEMS

    @classmethod
    def from_string(cls, data):
        parts = data.split("|")
        return cls.from_parts(parts)

    @classmethod
    def from_parts(cls, parts):
        if len(parts) < cls.PARTS:
            raise ValueError(
                f"Invalid TransactionItemRecord format: expected 6 fields, got {len(parts)}"
            )
        return cls(*parts)

    @classmethod
    def get_field_count(cls):
        return 6


class TransactionRecord(Record):
    """Transaction record: transaction_id, store_id, payment_method_id, voucher_id, user_id, original_amount, discount_applied, final_amount, created_at"""

    PARTS = 9

    def __init__(
        self,
        transaction_id,
        store_id,
        payment_method_id,
        voucher_id,
        user_id,
        original_amount,
        discount_applied,
        final_amount,
        created_at,
    ):
        self.transaction_id = transaction_id
        self.store_id = store_id
        self.payment_method_id = payment_method_id
        self.voucher_id = voucher_id
        self.user_id = user_id
        self.original_amount = original_amount
        self.discount_applied = discount_applied
        self.final_amount = final_amount
        self.created_at = created_at

    def serialize(self):
        return f"{self.transaction_id}|{self.store_id}|{self.payment_method_id}|{self.voucher_id}|{self.user_id}|{self.original_amount}|{self.discount_applied}|{self.final_amount}|{self.created_at}"

    def get_type(self):
        return DatasetType.TRANSACTIONS

    @classmethod
    def from_string(cls, data):
        parts = data.split("|")
        return cls.from_parts(parts)

    @classmethod
    def from_parts(cls, parts):
        if len(parts) < cls.PARTS:
            raise ValueError(
                f"Invalid TransactionRecord format: expected 9 fields, got {len(parts)}"
            )
        return cls(*parts)

    @classmethod
    def get_field_count(cls):
        return 9


class UserRecord(Record):
    """User record: user_id, gender, birthdate, registered_at"""

    PARTS = 4

    def __init__(self, user_id, gender, birthdate, registered_at):
        self.user_id = user_id
        self.gender = gender
        self.birthdate = birthdate
        self.registered_at = registered_at

    def serialize(self):
        return f"{self.user_id}|{self.gender}|{self.birthdate}|{self.registered_at}"

    def get_type(self):
        return DatasetType.USERS

    @classmethod
    def from_string(cls, data):
        parts = data.split("|")
        return cls.from_parts(parts)

    @classmethod
    def from_parts(cls, parts):
        if len(parts) < cls.PARTS:
            raise ValueError(
                f"Invalid UserRecord format: expected 4 fields, got {len(parts)}"
            )
        return cls(*parts)

    @classmethod
    def get_field_count(cls):
        return 4


class Q1Record(Record):
    """Q1 record: transaction_id, final_amount"""

    PARTS = 2

    def __init__(self, transaction_id, final_amount):
        self.transaction_id = transaction_id
        self.final_amount = final_amount

    def serialize(self):
        return f"{self.transaction_id}|{self.final_amount}"

    def get_type(self):
        return DatasetType.Q1

    @classmethod
    def from_string(cls, data):
        parts = data.split("|")
        return cls.from_parts(parts)

    @classmethod
    def from_parts(cls, parts):
        if len(parts) < cls.PARTS:
            raise ValueError(
                f"Invalid Q1Record format: expected 2 fields, got {len(parts)}"
            )
        return cls(*parts)

    @classmethod
    def get_field_count(cls):
        return 2


class Q2Record(Record):
    """Q2 record: year_month_created_at, item_name, sellings_qty"""

    PARTS = 3

    def __init__(self, year_month_created_at, item_name, sellings_qty):
        self.year_month_created_at = year_month_created_at
        self.item_name = item_name
        self.sellings_qty = sellings_qty

    def serialize(self):
        return f"{self.year_month_created_at}|{self.item_name}|{self.sellings_qty}"

    def get_type(self):
        return DatasetType.Q2

    @classmethod
    def from_string(cls, data):
        parts = data.split("|")
        return cls.from_parts(parts)

    @classmethod
    def from_parts(cls, parts):
        if len(parts) < cls.PARTS:
            raise ValueError(
                f"Invalid Q2Record format: expected 3 fields, got {len(parts)}"
            )
        return cls(*parts)

    @classmethod
    def get_field_count(cls):
        return 3


class Q3Record(Record):
    """Q3 record: year_half_created_at, store_name, tpv"""

    PARTS = 3

    def __init__(self, year_half_created_at, store_name, tpv):
        self.year_half_created_at = year_half_created_at
        self.store_name = store_name
        self.tpv = tpv

    def serialize(self):
        return f"{self.year_half_created_at}|{self.store_name}|{self.tpv}"

    def get_type(self):
        return DatasetType.Q3

    @classmethod
    def from_string(cls, data):
        parts = data.split("|")
        return cls.from_parts(parts)

    @classmethod
    def from_parts(cls, parts):
        if len(parts) < cls.PARTS:
            raise ValueError(
                f"Invalid Q3Record format: expected 3 fields, got {len(parts)}"
            )
        return cls(*parts)

    @classmethod
    def get_field_count(cls):
        return 3


class Q4Record(Record):
    """Q4 record: store_name, birthdate"""

    PARTS = 2

    def __init__(self, store_name, birthdate):
        self.store_name = store_name
        self.birthdate = birthdate

    def serialize(self):
        return f"{self.store_name}|{self.birthdate}"

    def get_type(self):
        return DatasetType.Q4

    @classmethod
    def from_string(cls, data):
        parts = data.split("|")
        return cls.from_parts(parts)

    @classmethod
    def from_parts(cls, parts):
        if len(parts) < cls.PARTS:
            raise ValueError(
                f"Invalid Q4Record format: expected 2 fields, got {len(parts)}"
            )
        return cls(*parts)

    @classmethod
    def get_field_count(cls):
        return 2


class BatchMessage:
    """Represents multiple records sent together for a specific dataset"""

    def __init__(self, dataset_type, records, eof=False):
        self.type = MESSAGE_TYPE_BATCH
        self.dataset_type = dataset_type  # DatasetType enum value
        self.records = records  # List of Record objects
        self.eof = eof

    @classmethod
    def from_data(cls, data):
        """Parse batch message from custom protocol data"""
        if len(data) < 2:
            raise ValueError("Invalid batch message: too short")

        if data[0] != MESSAGE_TYPE_BATCH:
            raise ValueError("Invalid batch message: not a batch message")

        dataset_type = data[1]
        content = data[2:].decode("utf-8")
        parts = content.split("|")

        logging.debug(
            f"action: parse_batch_message | dataset_type: {dataset_type} | content: {content}"
        )

        if len(parts) < 2:
            raise ValueError(
                "Invalid batch message format: missing EOF and record count"
            )

        eof = parts[0] == "1"
        record_count = int(parts[1])

        record_class = _get_record_class(dataset_type)
        fields_per_record = record_class.get_field_count()

        logging.debug(
            f"action: parse_batch_message | eof: {eof} | record_count: {record_count} | fields_per_record: {fields_per_record}"
        )

        data_parts = parts[2:]  # Skip EOF and record_count

        records = []
        for i in range(record_count):
            start_idx = i * fields_per_record
            end_idx = start_idx + fields_per_record

            if end_idx <= len(data_parts):
                record_fields = data_parts[start_idx:end_idx]

                logging.debug(
                    f"action: reconstruct_record | index: {i} | fields: {record_fields}"
                )

                try:
                    record = record_class.from_parts(record_fields)
                    records.append(record)
                    logging.debug(f"action: create_record | index: {i} | success: true")
                except Exception as e:
                    logging.error(f"action: create_record | index: {i} | error: {e}")

        return cls(dataset_type, records, eof)

class ResponseMessage:
    """Represents server response to client"""

    def __init__(self, success, error=None):
        self.type = MESSAGE_TYPE_RESPONSE
        self.success = success
        self.error = error


def _get_record_class(dataset_type):
    """Get the record class for a dataset type"""
    record_classes = {
        DatasetType.MENU_ITEMS: MenuItemRecord,
        DatasetType.STORES: StoreRecord,
        DatasetType.TRANSACTION_ITEMS: TransactionItemRecord,
        DatasetType.TRANSACTIONS: TransactionRecord,
        DatasetType.USERS: UserRecord,
        DatasetType.Q1: Q1Record,
        DatasetType.Q2: Q2Record,
        DatasetType.Q3: Q3Record,
        DatasetType.Q4: Q4Record,
    }

    record_class = record_classes.get(dataset_type)
    if not record_class:
        raise ValueError(f"Unknown dataset type: {dataset_type}")

    return record_class


def _create_record_from_string(dataset_type, data):
    """Factory function to create appropriate record type from string data"""
    record_class = _get_record_class(dataset_type)
    return record_class.from_string(data)
