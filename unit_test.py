import pytest
from code import code
import json


# creates a key-value pair.
def test_create():
    ds = code()
    key = "aaa"
    val = '{"ssn":1}'
    ttl = 0.001
    ds.create(key, val, ttl)
    assert ds.data[key]["value"] == val

def test_create1():
    ds = code()
    key = "fff"
    val = '{"ssn": 1}'
    ttl = 0
    ds.create(key, val, ttl)
    assert ds.data[key]["value"] == val


# creating a key that exists.
def test_create_exception():
    ds = code()
    key = "aaa"
    val = '{"ssn": 25}'
    ttl = 20
    with pytest.raises(Exception):
        ds.create(key, val, ttl)


# invalid key name
def test_create_exception1():
    ds = code()
    key = 12345
    val = '{"ssn": 25}'
    ttl = 20
    with pytest.raises(Exception):
        ds.create(key, val, ttl)


# key capped at greater than 32 chars.
def test_create_exception2():
    ds = code()
    key = "b" * 33
    val = '{"abcd": 1}'
    ttl = 20
    with pytest.raises(Exception):
        ds.create(key, val, ttl)


# value is not a JSON object.
def test_create_exception3():
    ds = code()
    key = "c"
    val = 'abcd'
    ttl = 20
    with pytest.raises(Exception):
        ds.create(key, val, ttl)


# reading a key.
def test_read():
    ds = code()
    key = "fff"
    val = '{"ssn": 1}'
    new_val=json.dumps(val)
    assert ds.read(key) == new_val


# reading a key which has expired.
def test_read_exception():
    ds = code()
    key = "aaa"
    with pytest.raises(Exception):
        ds.read(key)


# reading a key which does not exists.
def test_read_exception1():
    ds = code()
    key = "not"
    with pytest.raises(Exception):
        ds.read(key)


# deleting a key-value pair.
def test_delete():
    ds = code()
    key = "fff"
    ds.delete(key)
    assert key not in ds.data


# deleting a key which has expired.
def test_delete_exception():
    ds = code()
    key = "aaa"
    with pytest.raises(Exception):
        ds.delete(key)


# deleting a key which does not exists.
def test_delete_exception1():
    ds = code()
    key = "abcc"
    with pytest.raises(Exception):
        ds.delete(key)