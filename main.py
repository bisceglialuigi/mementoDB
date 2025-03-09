from memento_db import MementoDb

memento_db = MementoDb()

memento_db.put("key1", "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789")
value = memento_db.get("key1")
print(value)

memento_db.put("key2", "second value")
value = memento_db.get("key2")
print(value)

memento_db.put("key1", "first value overridden")
value = memento_db.get("key1")
print(value)

memento_db.delete("key1")
value = memento_db.get("key1")
print(value)