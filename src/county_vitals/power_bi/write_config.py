headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/113.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    # 'Accept-Encoding': 'gzip, deflate, br',
    'ActivityId': '394fc656-af31-b414-99c3-b3a86dd9d54b',
    'RequestId': 'ac250d1e-53d1-993a-ce93-686d404f372c',
    'X-PowerBI-ResourceKey': '2f5704a5-086e-4c69-a832-d003e64b7b55',
    'Content-Type': 'application/json;charset=UTF-8',
    'Origin': 'https://app.powerbigov.us',
    'Connection': 'keep-alive',
    'Referer': 'https://app.powerbigov.us/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'cross-site',
}

json_data = {
    'version': '1.0.0',
    'queries': [
        {
            'Query': {
                'Commands': [
                    {
                        'SemanticQueryDataShapeCommand': {
                            'Query': {
                                'Version': 2,
                                'From': [
                                    {
                                        'Name': 'm',
                                        'Entity': 'Measures_Table',
                                        'Type': 0,
                                    },
                                ],
                                'Select': [
                                    {
                                        'Measure': {
                                            'Expression': {
                                                'SourceRef': {
                                                    'Source': 'm',
                                                },
                                            },
                                            'Property': 'COVID Risk Level',
                                        },
                                        'Name': 'Measures_Table.COVID Risk Level',
                                    },
                                    {
                                        'Measure': {
                                            'Expression': {
                                                'SourceRef': {
                                                    'Source': 'm',
                                                },
                                            },
                                            'Property': 'rf_cdc_risk_level',
                                        },
                                        'Name': 'Measures_Table.rf_cdc_risk_level',
                                    },
                                ],
                            },
                            'Binding': {
                                'Primary': {
                                    'Groupings': [
                                        {
                                            'Projections': [
                                                0,
                                            ],
                                        },
                                    ],
                                },
                                'Projections': [
                                    1,
                                ],
                                'DataReduction': {
                                    'DataVolume': 3,
                                    'Primary': {
                                        'Top': {},
                                    },
                                },
                                'Version': 1,
                            },
                            'ExecutionMetricsKind': 1,
                        },
                    },
                ],
            },
            'CacheKey': '{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"m","Entity":"Measures_Table","Type":0}],"Select":[{"Measure":{"Expression":{"SourceRef":{"Source":"m"}},"Property":"COVID Risk Level"},"Name":"Measures_Table.COVID Risk Level"},{"Measure":{"Expression":{"SourceRef":{"Source":"m"}},"Property":"rf_cdc_risk_level"},"Name":"Measures_Table.rf_cdc_risk_level"}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0]}]},"Projections":[1],"DataReduction":{"DataVolume":3,"Primary":{"Top":{}}},"Version":1},"ExecutionMetricsKind":1}}]}',
            'QueryId': '',
            'ApplicationContext': {
                'DatasetId': '40047828-06d2-4b12-b499-fa0ac056cfb2',
                'Sources': [
                    {
                        'ReportId': '23713275-a563-4f00-a3ef-338c1e1751d8',
                        'VisualId': '049613bdc67d49e53be5',
                    },
                ],
            },
        },
        {
            'Query': {
                'Commands': [
                    {
                        'SemanticQueryDataShapeCommand': {
                            'Query': {
                                'Version': 2,
                                'From': [
                                    {
                                        'Name': 'm',
                                        'Entity': 'Measures_Table',
                                        'Type': 0,
                                    },
                                ],
                                'Select': [
                                    {
                                        'Measure': {
                                            'Expression': {
                                                'SourceRef': {
                                                    'Source': 'm',
                                                },
                                            },
                                            'Property': 'New Cases',
                                        },
                                        'Name': 'Measures_Table.New Cases',
                                    },
                                    {
                                        'Measure': {
                                            'Expression': {
                                                'SourceRef': {
                                                    'Source': 'm',
                                                },
                                            },
                                            'Property': 'rf_new_cases',
                                        },
                                        'Name': 'Measures_Table.rf_new_cases',
                                    },
                                ],
                            },
                            'Binding': {
                                'Primary': {
                                    'Groupings': [
                                        {
                                            'Projections': [
                                                0,
                                            ],
                                        },
                                    ],
                                },
                                'Projections': [
                                    1,
                                ],
                                'DataReduction': {
                                    'DataVolume': 3,
                                    'Primary': {
                                        'Top': {},
                                    },
                                },
                                'Version': 1,
                            },
                            'ExecutionMetricsKind': 1,
                        },
                    },
                ],
            },
            'CacheKey': '{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"m","Entity":"Measures_Table","Type":0}],"Select":[{"Measure":{"Expression":{"SourceRef":{"Source":"m"}},"Property":"New Cases"},"Name":"Measures_Table.New Cases"},{"Measure":{"Expression":{"SourceRef":{"Source":"m"}},"Property":"rf_new_cases"},"Name":"Measures_Table.rf_new_cases"}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0]}]},"Projections":[1],"DataReduction":{"DataVolume":3,"Primary":{"Top":{}}},"Version":1},"ExecutionMetricsKind":1}}]}',
            'QueryId': '',
            'ApplicationContext': {
                'DatasetId': '40047828-06d2-4b12-b499-fa0ac056cfb2',
                'Sources': [
                    {
                        'ReportId': '23713275-a563-4f00-a3ef-338c1e1751d8',
                        'VisualId': 'ea074f22133c07b37dc3',
                    },
                ],
            },
        },
        {
            'Query': {
                'Commands': [
                    {
                        'SemanticQueryDataShapeCommand': {
                            'Query': {
                                'Version': 2,
                                'From': [
                                    {
                                        'Name': 'm',
                                        'Entity': 'Measures_Table',
                                        'Type': 0,
                                    },
                                ],
                                'Select': [
                                    {
                                        'Measure': {
                                            'Expression': {
                                                'SourceRef': {
                                                    'Source': 'm',
                                                },
                                            },
                                            'Property': 'Cumulative Cases',
                                        },
                                        'Name': 'Measures_Table.Cumulative Cases',
                                    },
                                    {
                                        'Measure': {
                                            'Expression': {
                                                'SourceRef': {
                                                    'Source': 'm',
                                                },
                                            },
                                            'Property': 'rf_cumulative_cases',
                                        },
                                        'Name': 'Measures_Table.rf_cumulative_cases',
                                    },
                                ],
                            },
                            'Binding': {
                                'Primary': {
                                    'Groupings': [
                                        {
                                            'Projections': [
                                                0,
                                            ],
                                        },
                                    ],
                                },
                                'Projections': [
                                    1,
                                ],
                                'DataReduction': {
                                    'DataVolume': 3,
                                    'Primary': {
                                        'Top': {},
                                    },
                                },
                                'Version': 1,
                            },
                            'ExecutionMetricsKind': 1,
                        },
                    },
                ],
            },
            'CacheKey': '{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"m","Entity":"Measures_Table","Type":0}],"Select":[{"Measure":{"Expression":{"SourceRef":{"Source":"m"}},"Property":"Cumulative Cases"},"Name":"Measures_Table.Cumulative Cases"},{"Measure":{"Expression":{"SourceRef":{"Source":"m"}},"Property":"rf_cumulative_cases"},"Name":"Measures_Table.rf_cumulative_cases"}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0]}]},"Projections":[1],"DataReduction":{"DataVolume":3,"Primary":{"Top":{}}},"Version":1},"ExecutionMetricsKind":1}}]}',
            'QueryId': '',
            'ApplicationContext': {
                'DatasetId': '40047828-06d2-4b12-b499-fa0ac056cfb2',
                'Sources': [
                    {
                        'ReportId': '23713275-a563-4f00-a3ef-338c1e1751d8',
                        'VisualId': '8820771ad58084287250',
                    },
                ],
            },
        },
    ],
    'cancelQueries': [],
    'modelId': 382720,
}

url_dict = {
    'headers': headers,
    'payload': json_data
}

import yaml

with open('src/county_vitals/power_bi/temp_config.yaml', 'w') as f:
    yaml.dump(url_dict, f)
