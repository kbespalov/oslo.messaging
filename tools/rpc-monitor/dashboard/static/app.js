var app = angular.module('rpcstate', ['ngResource', 'chart.js']);


app.factory('RPCStateService', RPCStateService);

function RPCStateService($resource) {
    return $resource('/api/global_state', {}, {
        actual_state: {
            method: 'GET',
            params: {
                dest: 'stat'
            }
        },
        ping: {
            method: 'POST'
        },
        params: {
            dest: 'ping'
        },
        history: {
            method: 'GET',
            params: {
                dest: 'history'
            }
        }
    });
}


app.controller('RPCStateController', RPCStateController);


function RPCStateController($scope, $http, RPCStateService) {


    $scope.actual_state = {};
    $scope.pings = {};
    $scope.grouped_by_methods = {};
    $scope.grouped_by_workers = {};
    $scope.filter_query = "";

    $scope.do_filter = function (sample) {
        if ($scope.filter_query == "") {
            return true;
        }
        var calls = sample.calls;
        var host = sample.host;
        var worker = sample.worker;
        var endpoint = sample.endpoint;
        var method = sample.method;
        res = eval($scope.filter_query);

        return res;
    };

    function format_date(timestamp_in_seconds) {
        if (timestamp_in_seconds == 0) {
            return "-";
        }
        var date = new Date(timestamp_in_seconds * 1000);
        return date.getHours() + ":" + date.getMinutes() + ":" + date.getSeconds()
    }

    function generate_labels(granularity, loop_time, last_action) {
        if (last_action == 0)
            last_action = new Date().getSeconds();

        var upper_bound = last_action + (granularity - last_action % granularity);
        var lower_bound = upper_bound - loop_time;
        var labels = [];
        while (lower_bound < upper_bound) {
            labels.push(format_date(lower_bound));
            lower_bound += granularity;
        }
        return labels;
    }

    function calc_avg(method_stat) {
        if (method_stat.cs.total != 0) {
            return method_stat.ts.total / method_stat.cs.total;
        } else
            return 0;
    }

    function get_topic_workers(topic_name) {
        var workers = [];
        if ($scope.actual_state.hasOwnProperty(topic_name)) {
            var topic_stat = $scope.actual_state[topic_name];
            for (var host_name in topic_stat) {
                var host_stat = topic_stat[host_name];
                for (var worker_pid in host_stat) {
                    var worker_stat = host_stat[worker_pid];
                    var executor = worker_stat['executor'];
                    workers.push({
                        'host': host_name,
                        'wid': worker_pid,
                        'pong_time': format_date(worker_stat['last_sample']),
                        'latency': (worker_stat['last_sample'] - worker_stat['request_time']).toFixed(3),
                        'avg': executor['avg_runtime'].toFixed(3),
                        'executed': executor['executed'],
                        'runtime': format_date(executor['runtime'] - 3 * 3600)
                    });

                }
            }
        }
        return workers;
    }

    function get_topic_methods(topic_name) {
        var methods = [];
        if ($scope.actual_state.hasOwnProperty(topic_name)) {
            var topic_stat = $scope.actual_state[topic_name];
            for (var host_name in topic_stat) {
                var host_stat = topic_stat[host_name];
                for (var worker_pid in host_stat) {
                    var worker_stat = host_stat[worker_pid];
                    var loop_time = worker_stat['loop_time'];
                    var granularity = worker_stat['granularity'];
                    for (var enpoint_name in worker_stat.endpoints) {
                        var endpoint_methods = worker_stat.endpoints[enpoint_name];
                        for (var method_name in endpoint_methods) {
                            var method_stat = endpoint_methods[method_name];
                            var last_action = method_stat.cs['last_action'];
                            var labels = generate_labels(granularity, loop_time, last_action);
                            methods.push({
                                'host': host_name,
                                'wid': worker_pid,
                                'endpoint': enpoint_name,
                                'last_action': format_date(last_action),
                                'method': method_name,
                                'calls': method_stat.cs.total,
                                'total_time': method_stat.ts.total.toFixed(3),
                                'min_time': method_stat.ts['min'].toFixed(3),
                                'max_time': method_stat.ts['max'].toFixed(3),
                                'avg_time': calc_avg(method_stat).toFixed(3),
                                'cs': method_stat.cs.distribution,
                                'ts': method_stat.ts.distribution,
                                'labels': labels
                            });
                        }
                    }
                }
            }
        }
        return methods;
    }

    function loadData() {
        var data = RPCStateService.actual_state();
        data.$promise.then(function (result) {
            var response = result.toJSON();
            $scope.actual_state = response
            for (var topic in response) {
                $scope.grouped_by_workers[topic] = get_topic_workers(topic);
                $scope.grouped_by_methods[topic] = get_topic_methods(topic);
            }
        });
        // $("#preloader").fakeLoader({
        //     timeToHide: 500,
        //     spinner: "spinner3",
        //     bgColor: "#00E1FF"
        // });


    }

    loadData();
}
