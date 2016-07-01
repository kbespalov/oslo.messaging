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
function RPCStateController($scope, $http, RPCStateService) {


    $scope.methods_state = {};
    $scope.pings = {};
    $scope.filter_query = "";
    $scope.enable_table_sorting = function(){
    $.bootstrapSortable(true);
    }

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

    function load_method_state(sample) {
        RPCStateService.method_metrics({}, sample, function (entry) {
              var data = entry.toJSON();
              console.log(data);
              // chart values for current loop
              sample['ats'] = data['ats'];
              sample['cs'] = data['cs'];
              // just update current state for row
              sample['min'] = data['metrics']['min'];
              sample['max'] = data['metrics']['max'];
              sample['avg'] = data['metrics']['avg'];
              sample['calls'] = data['metrics']['calls'];
              sample['dev'] = data['metrics']['dev'];
              sample['last_call'] = data['metrics']['last_call']

              sample['labels'] = data['labels'];
              // runtime values
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
        load_method_state(sample);
        $('#'+sample.id).collapse("toggle");
        $.bootstrapSortable(true);
    };

    load_methods_state();
}
