//PREPARACIÓN DE COLECCIÓN Y DATOS DE EJEMPLO

use MapReduce

db.createCollection("pedidos")

db.pedidos.insert(
    [
        {
        _id: 1,
        store_name: "Supermercado XYZ",
        purchase_date: new Date("2023-04-27"),
        total_amount: 9.0,
        items: [ { item: "pan", qty: 2, price: 1.5 }, { item: "manzanas", qty: 5, price: 1.2 } ],
        payment_method: "tarjeta de crédito"
        },
        {
        _id: 2,
        store_name: "Supermercado XYZ",
        purchase_date: new Date("2023-05-11"),
        total_amount: 8.8,
        items: [ { item: "leche", qty: 1, price: 2.5 }, { item: "pan", qty: 1, price: 1.5 }, { item: "huevos", qty: 6, price: 0.8 } ],
        payment_method: "efectivo"
        },
        {
        _id: 3,
        store_name: "Supermercado ABC",
        purchase_date: new Date("2023-05-03"),
        total_amount: 6.0,
        items: [ { item: "manzanas", qty: 4, price: 1.5 } ],
        payment_method: "tarjeta de crédito"
        },
        {
        _id: 4,
        store_name: "Supermercado ABC",
        purchase_date: new Date("2023-05-14"),
        total_amount: 13.0,
        items: [ { item: "leche", qty: 2, price: 2.3 }, { item: "huevos", qty: 12, price: 0.7 } ],
        payment_method: "efectivo"
        },
        {
        _id: 5,
        store_name: "Supermercado XYZ",
        purchase_date: new Date("2023-05-17"),
        total_amount: 12.3,
        items: [ { item: "leche", qty: 3, price: 2.5 }, { item: "huevos", qty: 6, price: 0.8 } ],
        payment_method: "tarjeta de crédito"
        },
        {
        _id: 6,
        store_name: "Supermercado XYZ",
        purchase_date: new Date("2023-05-29"),
        total_amount: 7.1,
        items: [ { item: "leche", qty: 1, price: 2.5 }, { item: "pan", qty: 2, price: 1.5 }, { item: "huevos", qty: 2, price: 0.8 } ],
        payment_method: "efectivo"
        },
        {
        _id: 7,
        store_name: "Supermercado DEF",
        purchase_date: new Date("2023-05-03"),
        total_amount: 5.6,
        items: [ { item: "manzanas", qty: 4, price: 1.4 } ],
        payment_method: "tarjeta de crédito"
        },
        {
        _id: 8,
        store_name: "Supermercado DEF",
        purchase_date: new Date("2023-05-14"),
        total_amount: 17.4,
        items: [ { item: "leche", qty: 3, price: 2.2 }, { item: "huevos", qty: 12, price: 0.9 } ],
        payment_method: "efectivo"
        },
        {
        _id: 9,
        store_name: "Supermercado XYZ",
        purchase_date: new Date("2023-05-17"),
        total_amount: 14.6,
        items: [ { item: "leche", qty: 2, price: 2.5 }, { item: "huevos", qty: 12, price: 0.8 } ],
        payment_method: "tarjeta de crédito"
        },
        {
        _id: 10,
        store_name: "Supermercado XYZ",
        purchase_date: new Date("2023-05-29"),
        total_amount: 7.8,
        items: [ { item: "pan", qty: 2, price: 1.5 }, { item: "huevos", qty: 6, price: 0.8 } ],
        payment_method: "efectivo"
        }
    ]
)


//EJEMPLO SIMPLE

//ALTERNATIVA MAPREDUCE
var mapFunction1 = function() {
    emit(this.store_name, this.total_amount);
 };
 
 var reduceFunction1 = function(keyStore, valuesPrices) {
    return Array.sum(valuesPrices);
 };
 
 db.pedidos.mapReduce(
    mapFunction1,
    reduceFunction1,
    { out: "map_reduce_1" }
 )

//ALTERNATIVA AGGREGATION PIPELINE
 
 db.pedidos.aggregate([
    { $group: { _id: "$store_name", value: { $sum: "$total_amount" } } },
    { $out: "agg_alternative_1" }
 ])

//EJEMPLO INCREMENTAL

//ALTERNATIVA MAPREDUCE

var mapFunction2 = function() {
    this.items.forEach(function (item) {
       emit(item.item, { qty: item.qty, total_amount: (item.qty * item.price), count: 1});
    })
 };
 
 
 var reduceFunction2 = function (key, values) {
    var reducedValue = { qty: 0, total_amount: 0, count: 0};
    values.forEach(function (value) {
        reducedValue.qty += value.qty;
        reducedValue.total_amount += value.total_amount;
        reducedValue.count += value.count;
    });
    return reducedValue;
 };
 
 var finalizeFunction = function(key, reducedValue) {
    reducedValue.total_amount = Number(reducedValue.total_amount.toFixed(2));
    return reducedValue
 };
 
 db.pedidos.mapReduce(
    mapFunction2,
    reduceFunction2,
    { 
       out: "map_reduce_2",
       finalize: finalizeFunction,
    }
 );
 
 db.pedidos.insert(
    [
        {
        _id: 11,
        store_name: "Supermercado ABC",
        purchase_date: new Date("2023-06-01"),
        total_amount: 6.2,
        items: [ { item: "pan", qty: 2, price: 1.6 }, { item: "manzanas", qty: 2, price: 1.5 } ],
        payment_method: "tarjeta de crédito"
        },
    ]
 )
 
 db.pedidos.mapReduce(
    mapFunction2,
    reduceFunction2,
    { 
       query: {purchase_date: {$gte: new Date("2023-06-01")}},
       out: {reduce: "map_reduce_2"},
        finalize: finalizeFunction,
    }
 );
 
 db.pedidos.deleteOne({_id: 11})

//ALTERNATIVA AGGREGATION PIPELINE
 
 db.pedidos.aggregate([
    { $unwind: "$items" },
    { $project: {
          _id: "$items.item",
          qty: "$items.qty",
          total_amount: { $multiply: ["$items.qty", "$items.price"] },
          count: 1
       }
    },
    { $group: {
          _id: "$_id",
          qty: { $sum: "$qty" },
          total_amount: { $sum: "$total_amount" },
          count: { $sum: 1 }
       }
    },
    { $project: {
          _id: "$_id",
          value: {
             qty: "$qty",
             total_amount: { $round: ["$total_amount", 2] },
             count: "$count"
          }
       }
    },
    { $merge: {
       into: "alternative_2",
       whenMatched: [ { $set: {
          "value.qty": { $add: [ "$value.qty", "$$new.value.qty" ] },
          "value.total_amount": { $add: [ "$value.total_amount", "$$new.value.total_amount" ] },
          "value.count": { $add: [ "$value.count", "$$new.value.count" ] }
       } } ],
       whenNotMatched: "insert"
    }}
 ]);
 
 db.pedidos.insert(
    [
        {
        _id: 11,
        store_name: "Supermercado ABC",
        purchase_date: new Date("2023-06-01"),
        total_amount: 6.2,
        items: [ { item: "pan", qty: 2, price: 1.6 }, { item: "manzanas", qty: 2, price: 1.5 } ],
        payment_method: "tarjeta de crédito"
        },
    ]
 )
 
 db.pedidos.aggregate([
    { $match: { purchase_date: { $gte: new Date("2023-06-01") } } },
    { $unwind: "$items" },
    { $project: {
          _id: "$items.item",
          qty: "$items.qty",
          total_amount: { $multiply: ["$items.qty", "$items.price"] },
          count: 1
       }
    },
    { $group: {
          _id: "$_id",
          qty: { $sum: "$qty" },
          total_amount: { $sum: "$total_amount" },
          count: { $sum: 1 }
       }
    },
    { $project: {
          _id: "$_id",
          value: {
             qty: "$qty",
             total_amount: { $round: ["$total_amount", 2] },
             count: "$count"
          }
       }
    },
    { $merge: {
       into: "alternative_2",
       whenMatched: [ { $set: {
          "value.qty": { $add: [ "$value.qty", "$$new.value.qty" ] },
          "value.total_amount": { $add: [ "$value.total_amount", "$$new.value.total_amount" ] },
          "value.count": { $add: [ "$value.count", "$$new.value.count" ] }
       } } ],
       whenNotMatched: "insert"
    }}
 ]);
 
 