import json
import collections
import rawes


BulkAction = collections.namedtuple("BulkAction", ['action', 'operation'])


es = rawes.Elastic('localhost:9200')
index_name = "dev"


def build_bulk_request_payload(actions):
    assert actions
    ret = u''
    for action in actions:
        assert isinstance(action, BulkAction)
        ret += json.dumps(action.action) + u'\n' + json.dumps(action.operation) + u'\n'
    return ret + u'\n'


def do_bulk_request(url, actions):
    data = build_bulk_request_payload(actions)
    print data
    return es.post(url, data=data)


def test_bulk_request():
    response = do_bulk_request(
        url='{0}/file/_bulk'.format(index_name),
        actions=[
            BulkAction(
                action={"index": {}},
                operation={"key": "value4"}
            ),
        ]
    )
    print response

    new_item_id = response['items'][0]['create']['_id']
    print "-->", new_item_id

    response = do_bulk_request(
        url='{0}/file/_bulk'.format(index_name),
        actions=[
            BulkAction(
                action={"update": {"_index": "dev", "_type": "file", "_id": new_item_id}},
                operation={"params": {"param1": True}, "script": "ctx._source.was_edited = param1"}
            ),
        ]
    )

    print response


if __name__ == '__main__':
    test_bulk_request()
