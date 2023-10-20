from bson import ObjectId
from flask import Flask, render_template, jsonify, url_for, request
from pymongo import MongoClient
from datetime import datetime
import calendar

app = Flask(__name__)

mongoclient = MongoClient("mongodb://localhost:27017")
db = mongoclient["str4"]


@app.route("/")
def index():
    return render_template("dashboard.html")
    # return render_template("dashboard.html")




@app.route("/week_data/", methods=["GET", "POST"])
def get_week_data():
    try:
        data = request.get_json()
        start_date = data["startdate"]
        end_date = data["enddate"]
        str_id = data["str_id"]
        obj = db.str_reports.find_one({"str_id": str_id})
        # print(obj)
        str_id_objId = obj["_id"]

        collection_rank_mapping = {
            "adr": "adr_ss_ranks",
            "occupancy": "occupancy_ss_ranks",
            "revpar": "revpar_ss_ranks"
        }

        result = {}

        for collection_name in collection_rank_mapping.keys():
            lookup_collection = collection_rank_mapping[collection_name]

            pipeline = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime.fromisoformat(start_date),
                            "$lte": datetime.fromisoformat(end_date)
                        },
                        "metadata.str_id": ObjectId(str_id_objId)
                    }
                },
                {
                    "$sort": {
                        "timestamp": 1
                    }
                },
                {
                    "$lookup": {
                        "from": lookup_collection,
                        "localField": "timestamp",
                        "foreignField": "timestamp",
                        "as": "rank_data"
                    }
                },
                {
                    "$unwind": {
                        "path": "$rank_data",
                        "preserveNullAndEmptyArrays": True
                    }
                },
                {
                    "$match": {
                        "$or": [
                            {"rank_data.metadata.label": "Your rank"},
                            {"rank_data": {"$exists": False}}
                        ]
                    }
                },
                {
                    "$group": {
                        "_id": "$timestamp",
                        "rank": {
                            "$first": {"$concat": [
                                {"$toString": {"$arrayElemAt": [
                                    "$rank_data.rank", 0]}},
                                " of ",
                                {"$toString": {"$arrayElemAt": [
                                    "$rank_data.rank", 1]}}
                            ]}
                        },
                        "data": {
                            "$push": {
                                "label": "$metadata.label",
                                "change": "$change",
                                "change_rate": "$change_rate"
                            }
                        }
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "timestamp": "$_id",
                        "rank": 1,
                        "data": 1
                    }
                },
                {
                    "$sort": {
                        "timestamp": 1
                    }
                }
            ]

            collection = db[collection_name]

            result.update({collection_name: list(
                collection.aggregate(pipeline))})

        # print(result)

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})
    



@app.route("/month_data/", methods=["GET", "POST"])
def get_month_data():
    try:
        # print(data)
        data = request.get_json()
        year = int(data["year"])
        month = int(data["month"])
        # day = datetime.now().day
        year_upto = year+1
        from_year = year_upto - 3
        

        str_id = data["str_id"]
        obj = db.str_reports.find_one({"str_id": str_id})
        # print(obj)
        str_id_objId = obj["_id"]

        collection_rank_mapping = {
            "adr": "adr_ss_ranks",
            "occupancy": "occupancy_ss_ranks",
            "revpar": "revpar_ss_ranks"
        }

        result1 = {}
        total_yearToDate_result = {}
        total_Running3Month_result = {}
        total_Running12Month_result = {}
        final_result ={}

        for collection_name in collection_rank_mapping.keys():
            rank_collection = collection_rank_mapping[collection_name]
            toYear = year
            toMonth = month
            max_days = calendar.monthrange(toYear, toMonth)[1]
            toDay = max_days
            if month >= 3 :
                fromYear = year 
                fromMonth = toMonth-2
                fromDay = 1
            else:
                fromYear = year-1
                fromMonth = 10 + toMonth
                fromDay = 1
            

            pipeline = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime(fromYear, fromMonth, fromDay),
                            "$lte": datetime(toYear, toMonth, toDay)
                        },
                        "metadata.str_id": ObjectId(str_id_objId)

                    }
                },
                {
                    "$group": {
                        "_id": {
                            "year": {"$year": "$timestamp"},
                            "month": {"$month": "$timestamp"},
                            "label": "$metadata.label"
                        },
                        "avg_change": {"$avg": "$change"}


                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "timestamp": {
                            "$dateFromParts": {
                                "year": "$_id.year",
                                "month": "$_id.month"
                            }

                        },
                        "label": "$_id.label",
                        "avg_change": "$avg_change"
                    }
                },
                {
                    "$sort": {"timestamp": 1}
                }

            ]

            pipeline1 = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime(fromYear, fromMonth, fromDay),
                            "$lte": datetime(toYear, toMonth, toDay)
                        },
                        "metadata.str_id": ObjectId(str_id_objId)

                    }
                },
                {
                    "$group": {
                        "_id": {
                            "year": {"$year": "$timestamp"},
                            "month": {"$month": "$timestamp"}
                        },
                        "rank_avg_numerator": {
                            "$avg": {
                                "$arrayElemAt": [
                                    "$rank",
                                    0
                                ]
                            }
                        },
                        "rank_avg_denominator":{
                            "$avg": {
                                "$arrayElemAt": [
                                    "$rank",
                                    1
                                ]
                            }
                        }
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "month": {
                            "$dateFromParts": {
                                "year": "$_id.year",
                                "month": "$_id.month"
                            }
                        },
                        "month_rank_avg": {
                            "$concat": [
                                {"$toString": {"$round": "$rank_avg_numerator"}},
                                " of ",
                                {"$toString": {"$round": "$rank_avg_denominator"}}
                            ]
                        },


                    }
                }
            ]

            collection = db[collection_name]
            rank_collection = db[rank_collection]
            change_coll = collection_name+"_"+"change"
            rank_coll = collection_name+"_"+"ranks"
            result_here = {}
            result_here.update(
                {change_coll: list(collection.aggregate(pipeline))})
            result_here.update(
                {rank_coll: list(rank_collection.aggregate(pipeline1))})
            result1.update({collection_name: result_here})        
        final_result.update({year:result1})
        
        for loopingYear in range(from_year,year_upto):

            one_yearToDate_result = {}

            for collection_name in collection_rank_mapping.keys():
                rank_collection = collection_rank_mapping[collection_name]
                fromYear = loopingYear
                fromMonth = 1
                fromDay = 1
                toYear = loopingYear
                toMonth = datetime.now().month
                toDay = datetime.now().day
                print(type(fromYear),type(fromMonth),type(fromDay),type(toYear),type(toMonth),type(toDay))
                print(fromYear,fromMonth,fromDay,toYear,toMonth,toDay)
                # print(f"year: {year}, month: {month}, toMonth: {toMonth}, toDay: {toDay} , strid:{str_id_objId}")
                print(datetime(fromYear, fromMonth, fromDay))
                print(datetime(toYear, toMonth, toDay))
                pipeline = [
                    {
                        "$match": {
                            "timestamp": {
                                "$gte": datetime(fromYear, fromMonth, fromDay),
                                "$lte": datetime(toYear, toMonth, toDay)
                            },
                            "metadata.str_id": ObjectId(str_id_objId)

                        }
                    },
                    {
                        "$group": {
                            "_id": {
                                # "year": {"$year": "$timestamp"},
                                
                                "label": "$metadata.label"
                            },
                            "avg_change": {"$avg": "$change"},
                            "timestamp":{"$last":"$timestamp"}


                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "timestamp":1,
                            # "timestamp": {
                            #     "$dateFromParts": {
                            #         "year": "$_id.year",
                                    
                            #     }

                            # },
                            "label": "$_id.label",
                            "avg_change": "$avg_change"
                        }
                    },
                    {
                        "$sort": {"timestamp": 1}
                    }

                ]

                pipeline1 = [
                    {
                        "$match": {
                            "timestamp": {
                                "$gte": datetime(fromYear, fromMonth, fromDay),
                                "$lte": datetime(toYear, toMonth, toDay)
                            },
                            "metadata.str_id": ObjectId(str_id_objId)

                        }
                    },
                    {
                        "$group": {
                            # "_id": {
                            #     "year": {"$year": "$timestamp"},
                            #     # "month": {"$month": "$timestamp"}
                            # },
                            "_id":None,
                            "timestamp":{"$last":"$timestamp"},
                            "rank_avg_numerator": {
                                "$avg": {
                                    "$arrayElemAt": [
                                        "$rank",
                                        0
                                    ]
                                }
                            },
                            "rank_avg_denominator":{
                                "$avg": {
                                    "$arrayElemAt": [
                                        "$rank",
                                        1
                                    ]
                                }
                            }
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "timestamp":"$timestamp",
                            # "year": {
                            #     "$dateFromParts": {
                            #         "year": "$_id.year",
                            #         # "month": "$_id.month"
                            #     }
                            # },
                            "year_rank_avg": {
                                "$concat": [
                                    {"$toString": {"$round": "$rank_avg_numerator"}},
                                    " of ",
                                    {"$toString": {"$round": "$rank_avg_denominator"}}
                                ]
                            },


                        }
                    }
                ]

                collection = db[collection_name]
                rank_collection = db[rank_collection]
                change_coll = collection_name+"_"+"change"
                rank_coll = collection_name+"_"+"ranks"
                result_here = {}
                result_here.update(
                    {change_coll: list(collection.aggregate(pipeline))})
                result_here.update(
                    {rank_coll: list(rank_collection.aggregate(pipeline1))})
                one_yearToDate_result.update({collection_name: result_here})
            total_yearToDate_result.update({loopingYear:one_yearToDate_result})
        final_result.update({"Year To Date":total_yearToDate_result})

        # for loopingYear in range(from_year,year_upto):
            
        #     one_Running3Month_result = {}

        #     for collection_name in collection_rank_mapping.keys():
        #         rank_collection = collection_rank_mapping[collection_name]
                
        #         toYear = loopingYear
        #         toMonth = datetime.now().month 
        #         max_days = calendar.monthrange(toYear, toMonth)[1]
        #         toDay = max_days
                

        #         if toMonth >=3:
        #             fromYear = loopingYear
        #             fromMonth = toMonth-2
        #             fromDay = 1  
        #         else:
        #             fromYear = loopingYear-1
        #             fromMonth = 10 + toMonth
        #             fromDay = 1

                
        #         # print(f"year: {year}, month: {month}, toMonth: {toMonth}, toDay: {toDay} , strid:{str_id_objId}")
        #         pipeline = [
        #             {
        #                 "$match": {
        #                     "timestamp": {
        #                         "$gte": datetime(fromYear, fromMonth, fromDay),
        #                         "$lte": datetime(toYear, toMonth, toDay)
        #                     },
        #                     "metadata.str_id": ObjectId(str_id_objId)

        #                 }
        #             },
        #             {
        #                 "$group": {
        #                     "_id": {
        #                         # "year": {"$year": "$timestamp"},
                                
        #                         "label": "$metadata.label"
        #                     },
        #                     "avg_change": {"$avg": "$change"},
        #                     "timestamp":{"$last":"$timestamp"}


        #                 }
        #             },
        #             {
        #                 "$project": {
        #                     "_id": 0,
        #                     "timestamp":1,
        #                     # "timestamp": {
        #                     #     "$dateFromParts": {
        #                     #         "year": "$_id.year",
                                    
        #                     #     }

        #                     # },
        #                     "label": "$_id.label",
        #                     "avg_change": "$avg_change"
        #                 }
        #             },
        #             {
        #                 "$sort": {"timestamp": 1}
        #             }

        #         ]

        #         pipeline1 = [
        #             {
        #                 "$match": {
        #                     "timestamp": {
        #                         "$gte": datetime(fromYear, fromMonth, fromDay),
        #                         "$lte": datetime(toYear, toMonth, toDay)
        #                     },
        #                     "metadata.str_id": ObjectId(str_id_objId)

        #                 }
        #             },
        #             {
        #                 "$group": {
        #                     # "_id": {
        #                     #     "year": {"$year": "$timestamp"},
        #                     #     # "month": {"$month": "$timestamp"}
        #                     # },
        #                     "_id":None,
        #                     "timestamp":{"$last":"$timestamp"},
        #                     "rank_avg_numerator": {
        #                         "$avg": {
        #                             "$arrayElemAt": [
        #                                 "$rank",
        #                                 0
        #                             ]
        #                         }
        #                     },
        #                     "rank_avg_denominator":{
        #                         "$avg": {
        #                             "$arrayElemAt": [
        #                                 "$rank",
        #                                 1
        #                             ]
        #                         }
        #                     }
        #                 }
        #             },
        #             {
        #                 "$project": {
        #                     "_id": 0,
        #                     # "year": {
        #                     #     "$dateFromParts": {
        #                     #         "year": "$_id.year",
        #                     #         # "month": "$_id.month"
        #                     #     }
        #                     # },
        #                     "timestamp":"$timestamp",
        #                     "year_rank_avg": {
        #                         "$concat": [
        #                             {"$toString": {"$round": "$rank_avg_numerator"}},
        #                             " of ",
        #                             {"$toString": {"$round": "$rank_avg_denominator"}}
        #                         ]
        #                     },


        #                 }
        #             }
        #         ]

        #         collection = db[collection_name]
        #         rank_collection = db[rank_collection]
        #         change_coll = collection_name+"_"+"change"
        #         rank_coll = collection_name+"_"+"ranks"
        #         result_here = {}
        #         result_here.update(
        #             {change_coll: list(collection.aggregate(pipeline))})
        #         result_here.update(
        #             {rank_coll: list(rank_collection.aggregate(pipeline1))})
        #         one_Running3Month_result.update({collection_name: result_here})
        #     total_Running3Month_result.update({loopingYear:one_Running3Month_result})
        # final_result.update({"Running 3 Month":total_Running3Month_result})

        # for loopingYear in range(from_year,year_upto):
            
        #     one_Running12Month_result = {}

        #     for collection_name in collection_rank_mapping.keys():
        #         rank_collection = collection_rank_mapping[collection_name]
                
        #         toYear = loopingYear
        #         toMonth = datetime.now().month 
        #         max_days = calendar.monthrange(toYear, toMonth)[1]
        #         toDay = max_days
                

        #         if toMonth == 12 :
        #             fromYear = loopingYear
        #             fromMonth = 1
        #             fromDay = 1  
        #         else:
        #             fromYear = loopingYear-1
        #             fromMonth = toMonth + 1
        #             fromDay = 1

                
        #         # print(f"year: {year}, month: {month}, toMonth: {toMonth}, toDay: {toDay} , strid:{str_id_objId}")
        #         pipeline = [
        #             {
        #                 "$match": {
        #                     "timestamp": {
        #                         "$gte": datetime(fromYear, fromMonth, fromDay),
        #                         "$lte": datetime(toYear, toMonth, toDay)
        #                     },
        #                     "metadata.str_id": ObjectId(str_id_objId)

        #                 }
        #             },
        #             {
        #                 "$group": {
        #                     "_id": {
        #                         # "year": {"$year": "$timestamp"},
                                
        #                         "label": "$metadata.label"
        #                     },
        #                     "avg_change": {"$avg": "$change"},
        #                     "timestamp":{"$last":"$timestamp"}


        #                 }
        #             },
        #             {
        #                 "$project": {
        #                     "_id": 0,
        #                     "timestamp":1,
        #                     # "timestamp": {
        #                     #     "$dateFromParts": {
        #                     #         "year": "$_id.year",
                                    
        #                     #     }

        #                     # },
        #                     "label": "$_id.label",
        #                     "avg_change": "$avg_change"
        #                 }
        #             },
        #             {
        #                 "$sort": {"timestamp": 1}
        #             }

        #         ]

        #         pipeline1 = [
        #             {
        #                 "$match": {
        #                     "timestamp": {
        #                         "$gte": datetime(fromYear, fromMonth, fromDay),
        #                         "$lte": datetime(toYear, toMonth, toDay)
        #                     },
        #                     "metadata.str_id": ObjectId(str_id_objId)

        #                 }
        #             },
        #             {
        #                 "$group": {
        #                     # "_id": {
        #                     #     "year": {"$year": "$timestamp"},
        #                     #     # "month": {"$month": "$timestamp"}
        #                     # },
        #                     "_id":None,
        #                     "timestamp":{"$last":"$timestamp"},
        #                     "rank_avg_numerator": {
        #                         "$avg": {
        #                             "$arrayElemAt": [
        #                                 "$rank",
        #                                 0
        #                             ]
        #                         }
        #                     },
        #                     "rank_avg_denominator":{
        #                         "$avg": {
        #                             "$arrayElemAt": [
        #                                 "$rank",
        #                                 1
        #                             ]
        #                         }
        #                     }
        #                 }
        #             },
        #             {
        #                 "$project": {
        #                     "_id": 0,
        #                     # "year": {
        #                     #     "$dateFromParts": {
        #                     #         "year": "$_id.year",
        #                     #         # "month": "$_id.month"
        #                     #     }
        #                     # },
        #                     "timestamp":"$timestamp",
        #                     "year_rank_avg": {
        #                         "$concat": [
        #                             {"$toString": {"$round": "$rank_avg_numerator"}},
        #                             " of ",
        #                             {"$toString": {"$round": "$rank_avg_denominator"}}
        #                         ]
        #                     },


        #                 }
        #             }
        #         ]

        #         collection = db[collection_name]
        #         rank_collection = db[rank_collection]
        #         change_coll = collection_name+"_"+"change"
        #         rank_coll = collection_name+"_"+"ranks"
        #         result_here = {}
        #         result_here.update(
        #             {change_coll: list(collection.aggregate(pipeline))})
        #         result_here.update(
        #             {rank_coll: list(rank_collection.aggregate(pipeline1))})
        #         one_Running12Month_result.update({collection_name: result_here})
        #     total_Running12Month_result.update({loopingYear:one_Running12Month_result})
        # final_result.update({"Running 12 Month":total_Running12Month_result})

        return jsonify(final_result)
   
    except Exception as e:
            return jsonify({"error": str(e)})


@app.route("/weekly_data/", methods=["GET", "POST"])
def get_weekly_data():
    try:
        data = request.get_json()
        week_start_date = data["week_start_date"]
        week_end_date = data["week_end_date"]
        str_id = data["str_id"]
        obj = db.str_reports.find_one({"str_id": str_id})
        # print(obj)
        str_id_objId = obj["_id"]

        collection_rank_mapping = {
            "adr": "adr_ss_ranks",
            "occupancy": "occupancy_ss_ranks",
            "revpar": "revpar_ss_ranks"
        }

        result = {}

        for collection_name in collection_rank_mapping.keys():
            rank_collection = collection_rank_mapping[collection_name]

            pipeline = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime.fromisoformat(week_start_date),
                            "$lte": datetime.fromisoformat(week_end_date)
                        },
                        "metadata.str_id": ObjectId(str_id_objId)

                    }
                },
                {
                    "$group": {
                        "_id": {
                            "year": {"$year": "$timestamp"},
                            "week": {"$week": "$timestamp"},
                            "label": "$metadata.label",
                        },
                        "avg_change": {"$avg": "$change"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "week_start_date": {
                            "$dateFromParts": {
                                "isoWeekYear": "$_id.year",
                                "isoWeek": "$_id.week",
                            }
                        },
                        "label": "$_id.label",
                        "avg_change": 1,
                    }
                },
                {"$sort": {"week_start_date": 1}},
                {
                    "$group": {
                        "_id": "$label",
                        "weekly_averages": {"$push": "$$ROOT"},
                        "total_weeks_average": {"$avg": "$avg_change"},
                    }
                },

            ]

            pipeline1 = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime.fromisoformat(week_start_date),
                            "$lte": datetime.fromisoformat(week_end_date)
                        },
                        "metadata.str_id": ObjectId(str_id_objId)


                    }
                },
                {
                    "$group": {
                        "_id": {
                            "year": {"$year": "$timestamp"},
                            "week": {"$week": "$timestamp"},

                        },
                        "rank_avg_numerator": {
                            "$avg": {
                                "$arrayElemAt": [
                                    "$rank",
                                    0
                                ]
                            }
                        },
                        "rank_avg_denominator":{
                            "$avg": {
                                "$arrayElemAt": [
                                    "$rank",
                                    1
                                ]
                            }
                        },
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "timestamp": {
                            "$dateFromParts": {
                                "isoWeekYear": "$_id.year",
                                "isoWeek": "$_id.week",
                                # "isoDayOfWeek": 0
                            }
                        },
                        "weekly_rank_avg": {
                            "$concat": [
                                {"$toString": {"$round": "$rank_avg_numerator"}},
                                " of ",
                                {"$toString": {"$round": "$rank_avg_denominator"}}
                            ]
                        },
                        "rank_avg_numerator": "$rank_avg_numerator",
                        "rank_avg_denominator": "$rank_avg_denominator"

                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "weekly_rank_avg": {"$push": "$$ROOT"},
                        "rank_avg_numerator": {"$avg": "$rank_avg_numerator"},
                        "rank_avg_denominator": {"$avg": "$rank_avg_denominator"}

                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "weekly_rank_avg": "$weekly_rank_avg",
                        "total_weekly_rank_avg": {
                            "$concat": [
                                {"$toString": {"$round": "$rank_avg_numerator"}},
                                " of ",
                                {"$toString": {"$round": "$rank_avg_denominator"}}
                            ]
                        },

                    }
                }
            ]
            collection = db[collection_name]
            rank_collection = db[rank_collection]
            change_coll = collection_name+"_"+"change"
            rank_coll = collection_name+"_"+"ranks"
            result_here = []
            result_here.append(
                {change_coll: list(collection.aggregate(pipeline))})
            result_here.append(
                {rank_coll: list(rank_collection.aggregate(pipeline1))})
            result.update({collection_name: result_here})
        # print(result)
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/monthly_data/", methods=["GET", "POST"])
def get_monthly_data():
    try:
        data = request.get_json()
        year = int(data["year_selected"])
        str_id = data["str_id"]
        obj = db.str_reports.find_one({"str_id": str_id})
        # print(obj)
        str_id_objId = obj["_id"]

        collection_rank_mapping = {
            "adr": "adr_ss_ranks",
            "occupancy": "occupancy_ss_ranks",
            "revpar": "revpar_ss_ranks"
        }

        result = {}

        for collection_name in collection_rank_mapping.keys():
            rank_collection = collection_rank_mapping[collection_name]
            pipeline = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime(year, 1, 1),
                            "$lt": datetime(year + 1, 1, 1)
                        },
                        "metadata.str_id": ObjectId(str_id_objId)
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "year": {"$year": "$timestamp"},
                            "month": {"$month": "$timestamp"},
                            "label": "$metadata.label"
                        },
                        "avg_change": {
                            "$avg": "$change"
                        }
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "month": {
                            "$dateFromParts": {
                                "year": "$_id.year",
                                "month": "$_id.month"
                            }
                        },
                        "label": "$_id.label",
                        "avg_change": 1
                    }
                },
                {
                    "$sort": {
                        "month": 1
                    }
                },
                {
                    "$group": {
                        "_id": "$label",
                        "monthly_averages": {"$push": "$$ROOT"},
                        "total_months_average": {"$avg": "$avg_change"}
                    }
                }
            ]

            pipeline1 = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime(year, 1, 1),
                            "$lt": datetime(year + 1, 1, 1)
                        },
                        "metadata.str_id": ObjectId(str_id_objId)
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "year": {"$year": "$timestamp"},
                            "month": {"$month": "$timestamp"},

                        },
                        "rank_avg_numerator": {
                            "$avg": {
                                "$arrayElemAt": [
                                    "$rank",
                                    0
                                ]
                            }
                        },
                        "rank_avg_denominator":{
                            "$avg": {
                                "$arrayElemAt": [
                                    "$rank",
                                    1
                                ]
                            }
                        },
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "month": {
                            "$dateFromParts": {
                                "year": "$_id.year",
                                "month": "$_id.month"
                            }
                        },
                        "monthly_rank_avg": {
                            "$concat": [
                                {"$toString": {"$round": "$rank_avg_numerator"}},
                                " of ",
                                {"$toString": {"$round": "$rank_avg_denominator"}}
                            ]
                        },
                        "rank_avg_numerator": "$rank_avg_numerator",
                        "rank_avg_denominator": "$rank_avg_denominator"

                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "monthly_rank_avg": {"$push": "$$ROOT"},
                        "rank_avg_numerator": {"$avg": "$rank_avg_numerator"},
                        "rank_avg_denominator": {"$avg": "$rank_avg_denominator"}

                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "monthly_rank_avg": "$monthly_rank_avg",
                        "total_monthly_rank_avg": {
                            "$concat": [
                                {"$toString": {"$round": "$rank_avg_numerator"}},
                                " of ",
                                {"$toString": {"$round": "$rank_avg_denominator"}}
                            ]
                        },

                    }
                }
            ]

            collection = db[collection_name]
            rank_collection = db[rank_collection]
            change_coll = collection_name+"_"+"change"
            rank_coll = collection_name+"_"+"ranks"
            result_here = []
            result_here.append(
                {change_coll: list(collection.aggregate(pipeline))})
            result_here.append(
                {rank_coll: list(rank_collection.aggregate(pipeline1))})
            result.update({collection_name: result_here})

        # print(result)
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/yearly_data/", methods=["GET", "POST"])
def get_yearly_data():
    try:
        data = request.get_json()
        num = int(data["years_selected"])
        current_year = datetime.now().year
        start_year = current_year-num
        # print(current_year,start_year)
        str_id = data["str_id"]
        obj = db.str_reports.find_one({"str_id": str_id})
        # print(obj)
        str_id_objId = obj["_id"]
        collection_rank_mapping = {
            "adr": "adr_ss_ranks",
            "occupancy": "occupancy_ss_ranks",
            "revpar": "revpar_ss_ranks"
        }

        result = {}

        for collection_name in collection_rank_mapping.keys():
            rank_collection = collection_rank_mapping[collection_name]

            pipeline = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime(start_year, 1, 1),
                            "$lte": datetime(current_year, 10, 1)
                        },
                        "metadata.str_id": ObjectId(str_id_objId)
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "year": {"$year": "$timestamp"},
                            "label": "$metadata.label"
                        },
                        "avg_change": {
                            "$avg": "$change"
                        }

                    }
                },
                {
                    "$sort": {
                        "year": -1
                    }
                }
            ]

            pipeline1 = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime(start_year, 1, 1),
                            "$lte": datetime(current_year, 10, 1)
                        },
                        "metadata.str_id": ObjectId(str_id_objId)
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "year": {"$year": "$timestamp"}
                        },
                        "rank_avg_numerator": {
                            "$avg": {
                                "$arrayElemAt": [
                                    "$rank",
                                    0
                                ]
                            }
                        },
                        "rank_avg_denominator":{
                            "$avg": {
                                "$arrayElemAt": [
                                    "$rank",
                                    1
                                ]
                            }
                        }
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "year": {
                            "$dateFromParts": {
                                "year": "$_id.year"
                            }
                        },
                        "yearly_rank_avg": {
                            "$concat": [
                                {"$toString": {"$round": "$rank_avg_numerator"}},
                                " of ",
                                {"$toString": {"$round": "$rank_avg_denominator"}}
                            ]
                        },


                    }
                },

            ]
            collection = db[collection_name]
            rank_collection = db[rank_collection]
            change_coll = collection_name+"_"+"change"
            rank_coll = collection_name+"_"+"ranks"
            result_here = []
            result_here.append(
                {change_coll: list(collection.aggregate(pipeline))})
            result_here.append(
                {rank_coll: list(rank_collection.aggregate(pipeline1))})
            result.update({collection_name: result_here})

        # print(result)

        return jsonify(result)
    except Exception as e:
        print(e)
        return jsonify({"error": str(e)})


@app.route("/range_data/", methods=["GET", "POST"])
def get_range_data():
    try:
        data = request.get_json()
        start_date = data["startdate"]
        end_date = data["enddate"]
        str_id = data["str_id"]
        obj = db.str_reports.find_one({"str_id": str_id})
        # print(obj)
        str_id_objId = obj["_id"]

        collection_rank_mapping = {
            "adr": "adr_ss_ranks",
            "occupancy": "occupancy_ss_ranks",
            "revpar": "revpar_ss_ranks"
        }

        result = {}

        for collection_name in collection_rank_mapping.keys():
            rank_collection = collection_rank_mapping[collection_name]
            pipeline = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime.fromisoformat(start_date),
                            "$lte": datetime.fromisoformat(end_date)
                        },
                        "metadata.str_id": ObjectId(str_id_objId)
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "label": "$metadata.label"
                        },
                        "avg_change": {
                            "$avg": "$change"
                        }
                    }
                },
                {
                    "$project": {
                        "label": "$_id.label",
                        "avg_change": "$avg_change",
                        "_id": 0
                    }
                }
            ]

            pipeline1 = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime.fromisoformat(start_date),
                            "$lte": datetime.fromisoformat(end_date)
                        },
                        "metadata.str_id": ObjectId(str_id_objId)
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "rank_avg_numerator": {
                            "$avg": {
                                "$arrayElemAt": [
                                    "$rank",
                                    0
                                ]
                            }
                        },
                        "rank_avg_denominator":{
                            "$avg": {
                                "$arrayElemAt": [
                                    "$rank",
                                    1
                                ]
                            }
                        }
                    }
                },
                {
                    "$project": {
                        "rank": {
                            "$concat": [
                                {"$toString": {"$round": "$rank_avg_numerator"}},
                                " of ",
                                {"$toString": {"$round": "$rank_avg_denominator"}}
                            ]
                        }
                    }
                }
            ]

            collection = db[collection_name]
            rank_collection = db[rank_collection]
            change_coll = collection_name+"_"+"change"
            rank_coll = collection_name+"_"+"ranks"
            result_here = []
            result_here.append(
                {change_coll: list(collection.aggregate(pipeline))})
            result_here.append(
                {rank_coll: list(rank_collection.aggregate(pipeline1))})
            result.update({collection_name: result_here})

        # print(result)

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})





if __name__ == "__main__":
    app.run(debug=True, port=8080)
    # app.run(debug=True, port=5500)
