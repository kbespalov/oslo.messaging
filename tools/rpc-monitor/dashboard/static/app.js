var app = angular.module('rpcstate', ['ngResource', 'chart.js']);


app.factory('RPCStateService', RPCStateService);

function RPCStateService($resource) {
    return $resource('/api/:resource/:action', {}, {
        global_state: {
            method: 'GET',
            params: {
                resource: 'global',
                action: 'state'
            }
        },
        methods_state: {
            method: 'GET',
            params: {
                resource: 'methods',
                action: 'state'
            }
        },
        method_metrics: {
            method: 'POST',
            params: {
                resource: 'method',
                action: 'state'
            }
        },
        workers_list: {
            method: 'GET',
            params: {
                resource: 'workers',
                action: ''
            }
        },
        workers_state: {
            method: 'GET',
            params: {
                resource: 'workers',
                action: 'state'
            }
        }
    });
}


app.controller('RPCStateController', RPCStateController);

function RPCStateController($scope, RPCStateService) {

    // controllers data
    $scope.methods_state = {};
    $scope.pings = {};

    // table view options
    $scope.filter_query = "";
    $scope.sortingOrder = {};


    $scope.sortBy = function (topic_name, key) {
        var sort = $scope.sortingOrder[topic_name];
        if (!sort) {
            $scope.sortingOrder[topic_name] = {'key': key, 'reverse': true};
        } else {
            $scope.sortingOrder[topic_name] = {'key': key, 'reverse': !sort['reverse']};
        }
    };

    $scope.getSortingKey = function (topic_name) {
        if (!$scope.sortingOrder.hasOwnProperty(topic_name)) {
            $scope.sortingOrder[topic_name] = {'key': 'calls', 'reverse': true};
            return 'calls';
        } else {
            return $scope.sortingOrder[topic_name].key;
        }
    };

    $scope.getSortingOrder = function (topic_name) {
        if (!$scope.sortingOrder.hasOwnProperty(topic_name)) {
            $scope.sortingOrder[topic_name] = {'key': 'calls', 'reverse': true};
            return true;
        } else {
            return $scope.sortingOrder[topic_name].reverse;
        }
    };

    function update_method_state(sample) {
        RPCStateService.method_metrics({}, sample, function (entry) {
            var data = entry.toJSON();
            console.log(data);
            // chart values for current loop
            sample['ats'] = data['ats'];
            sample['cs'] = data['cs'];
            sample['min'] = data['metrics']['min'];
            sample['max'] = data['metrics']['max'];
            sample['avg'] = data['metrics']['avg'];
            sample['calls'] = data['metrics']['calls'];
            sample['dev'] = data['metrics']['dev'];
            sample['last_call'] = data['metrics']['last_call'];
            sample['labels'] = data['labels'];
            sample['runtime'] = data['runtime']
        });
    }

    function load_methods_state() {
        return RPCStateService.methods_state(function (result) {
            $scope.methods_state = result.toJSON();
        });
    }

    $scope.show_charts = function (event, sample, topic) {
        sample['topic'] = topic;
        update_method_state(sample);
        $('#' + sample.id).collapse("toggle");
    };
    load_methods_state();
}
