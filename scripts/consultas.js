//consulta para saber que restaurantes tienen mas inspecciones 
db.inspections.aggregate([
    {
        $group: {
            _id: "$restaurant_id", total_inspections: { sum: 1 }
        }
    },
    { $sort: { total_inspections: -1 } },
    { $limit: 10 }
])

//hacer el embedding 
db.restaurants_with_inspections.insertMany(
  db.restaurants.aggregate([
    {
      $lookup: {
        from: "inspections",
        let: { restaurantId: "$_id" },
        pipeline: [
          {
            $match: {
              $expr: { $eq: [{ $toObjectId: "$restaurant_id" }, "$$restaurantId"] }
            }
          },
          {
            $project: {
              _id: 1,
              id: 1,
              certificate_number: 1,
              date: 1,
              result: 1,
              sector: 1
            }
          }
        ],
        as: "inspections"
      }
    },
    {
      $project: {
        _id: 1,
        URL: 1,
        address: 1,
        address_line_2: 1,
        name: 1,
        outcode: 1,
        postcode: 1,
        rating: 1,
        type_of_food: 1,
        inspections: 1
      }
    }
  ]).toArray()
);


//esquema de validación
db.runCommand({
    collMod: restaurants_with_inspections,
    validator: {
      $jsonSchema: {
        bsonType: "object",
        required: ["_id", "URL", "address", "name", "outcode", "postcode", "rating", "type_of_food", "inspections"],
        properties: {
          _id: { bsonType: "objectId" },
          URL: { bsonType: "string", pattern: "^https?:\\/\\/.*", description: "Debe ser una URL válida" },
          address: { bsonType: "string", description: "Dirección del negocio" },
          name: { bsonType: "string", description: "Nombre del negocio" },
          outcode: { bsonType: "string", description: "Código postal de salida" },
          postcode: { bsonType: "string", description: "Código postal" },
          rating: { 
            bsonType: "int", 
            minimum: 0, 
            maximum: 6, 
            description: "Valoración de 0 a 6"
          },
          type_of_food: { bsonType: "string", description: "Tipo de comida ofrecida" },
          inspections: {
            bsonType: "array",
            description: "Lista de inspecciones realizadas",
            items: {
              bsonType: "object",
              required: ["_id", "id", "certificate_number", "date", "result", "sector"],
              properties: {
                _id: { bsonType: "objectId" },
                id: { bsonType: "string", description: "ID de la inspección" },
                certificate_number: { bsonType: "int", description: "Número de certificado" },
                date: { 
                  bsonType: "string", 
                  pattern: "^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \\d{1,2} \\d{4}$",
                  description: "Fecha en formato 'Mes DD YYYY'"
                },
                result: {
                  bsonType: "string",
                  enum: ["Pass", "Fail", "Warning Issued", "Violation Issued", "No Violation Issued"],
                  description: "Resultado de la inspección"
                },
                sector: { bsonType: "string", description: "Sector de la inspección" }
              }
            }
          }
        }
      }
    },
    validationAction: "error"
  });

  
//buscar restaurantes tipo de comida especifico
db.restaurants.find({ type_of_food: "Chinese" });

//listar inspecciones con violaciones ordenadas por fecha
db.inspections.find({ result: "Violation Issued" }).sort({ date: 1 });

//encontrar restaurantes con una calificación superior a 4
db.restaurants.find({ rating: { $gt: 4 } });


//agrupar restaurantes por tipo de comida y calcular calificación promedio
db.restaurants.aggregate([
    {
      $group: {
        _id: "$type_of_food",         
        avg_rating: { $avg: "$rating" } 
      }
    }
  ])


//contar numero de inspecciones por resultado y mostrar porcentages 
db.inspections.aggregate([
    {
        $group: {
            _id: "$result",
            count: { $sum: 1 }
        }
    },

    {
        $lookup: {
            from: "inspections",
            pipeline: [
                { $count: "totalCount" }
            ],
            as: "total"
        }
    },
    {
        $unwind: "$total"
    },

    {
        $project: {
            _id: 0,
            result: "$_id",
            count: 1,
            percentage: {
                $multiply: [
                    { $divide: ["$count", "$total.totalCount"] }, 100
                ]
            }
        }
    },
    {
        $sort: { count: -1 }
    }
])


//restaurantes de un tipo de comida con estadisticas 
db.restaurants_with_inspections.find({ type_of_food: "Chinese" }).explain("executionStats")

//restaurantes con mayor rating de 5 con estadisticas 
db.restaurants_with_inspections.explain("executionStats").find({ rating: { $$gte: 5 } })

//Restaurantes con almenos una inspección con el resultado = 'Pass'
db.restaurants_with_inspections.find({ "inspections.result": "Pass"}).explain("executionStats")

//creacion indices
db.restaurants_with_inspections.createIndex({ type_of_food: 1})
db.restaurants_with_inspections.createIndex({ rating: 1 })
db.restaurants_with_inspections.createIndex({ "inspections.result": 1 })

//contar cuantos restaurantes hay de cada tipo de comida
db.restaurants.aggregate([
    {
      $group: {
        _id: "$type_of_food",  
        total_restaurants: { $sum: 1 }  
      }
    },
    {
      $sort: { total_restaurants: -1 }  
    }
  ])