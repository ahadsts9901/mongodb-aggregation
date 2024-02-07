import express from 'express';
import { client } from '../mongodb.mjs'
import { ObjectId } from 'mongodb';

const db = client.db("aggregation")
const users = db.collection("users")
const authors = db.collection("authors")
const books = db.collection("books")

let router = express.Router()

// problem 1: how many users are active ?
router.get('/problem-1', async (req, res) => {

    const result = await users.aggregate(
        [
            {
                $match: {
                    isActive: true,
                },
            },
            {
                $count: "activeUsers",
            },
        ]
    )
        .toArray()

    res.send({
        message: "data fetched",
        data: result
    })

})

// problem 2: what is the average age of all users ?
router.get('/problem-2', async (req, res) => {

    const result = await users.aggregate(
        [
            {
                $group: {
                    _id: null,
                    averageAge: {
                        $avg: "$age"
                    }
                }
            }
        ]
    ).toArray()

    res.send({
        message: "data fetched",
        data: result
    })

})

// problem 3: list the top 2 favourite fruits among the users ?
router.get('/problem-3', async (req, res) => {

    const result = await users.aggregate(
        [
            {
                $group: {
                    _id: "$favoriteFruit",
                    count: {
                        $sum: 1,
                    },
                },
            },
            {
                $sort: {
                    count: -1,
                },
            },
            {
                $limit: 2,
            },
        ]
    ).toArray()

    res.send({
        message: "data fetched",
        data: result
    })

})

// problem 4: find the total number of males and females ?
router.get('/problem-4', async (req, res) => {

    const result = await users.aggregate(
        [
            {
                $group: {
                    _id: "$gender",
                    count: {
                        $sum: 1
                    }
                }
            },
        ]
    ).toArray()

    res.send({
        message: "data fetched",
        data: result
    })

})

// problem 5: which country has the highest number of registered users ?
router.get('/problem-5', async (req, res) => {

    const result = await users.aggregate(
        [
            {
                $group: {
                    _id: "$company.location.country",
                    users: { $sum: 1 }
                }
            },
            {
                $sort: {
                    users: -1
                }
            },
            {
                $limit: 1
            }
        ]
    ).toArray()

    res.send({
        message: "data fetched",
        data: result
    })

})

// problem 6: list all unique eye colors present in the collection ?
router.get('/problem-6', async (req, res) => {

    const result = await users.aggregate(
        [
            {
                $group: {
                    _id: "$eyeColor",
                },
            },
        ]
    ).toArray()

    res.send({
        message: "data fetched",
        data: result
    })

})

// problem 7: what is the average number of tags per user ?

// method 1 : using 3 pipelines
router.get('/problem-7a', async (req, res) => {

    const result = await users.aggregate(
        [
            {
                $unwind: {
                    path: "$tags",
                }
            }, {
                $group: {
                    _id: "$_id",
                    numberOfTags: { $sum: 1 }
                }
            }, {
                $group: {
                    _id: null,
                    averageOfTagsPerUser: {
                        $avg: "$numberOfTags"
                    }
                }
            }
        ]
    ).toArray()

    res.send({
        message: "data fetched",
        data: result
    })

})

// method 2 : using 2 pipelines
router.get('/problem-7b', async (req, res) => {

    const result = await users.aggregate(
        [
            {
                $addFields: {
                    totalTagsPerUser: {
                        $size: {
                            $ifNull: ["$tags", []]
                        }
                    }
                }
            },
            {
                $group: {
                    _id: null,
                    averageOfTagsPerUser: {
                        $avg: "$totalTagsPerUser"
                    }
                }
            }
        ]
    ).toArray()

    res.send({
        message: "data fetched",
        data: result
    })

})

// problem 8: how many users have "enim" as one of their tags ?
router.get('/problem-8', async (req, res) => {

    const result = await users.aggregate(
        [
            {
                $match: {
                    tags: "enim"
                }
            }, {
                $count: 'usersWithEnimTag'
            }
        ]
    ).toArray()

    res.send({
        message: "data fetched",
        data: result
    })

})

// problem 9: what are the "names" and "age" of those users who are inactive and have "velit" as a tag ?
router.get('/problem-9', async (req, res) => {

    const result = await users.aggregate(
        [
            {
                $match: {
                    isActive: false,
                    tags: "velit"
                }
            },
            {
                $project: {
                    name: 1,
                    age: 1
                }
            }
        ]
    ).toArray()

    res.send({
        message: "data fetched",
        data: result
    })

})

// problem 10: how many users have a phone number starting with "+1 (940)" ?
router.get('/problem-10', async (req, res) => {

    const result = await users.aggregate(
        [
            {
                $match: {
                    "company.phone": /\+1\s\(940\)\s/
                }
            },
            {
                $count: "usersWithSpecialNumber"
            }
        ]
    ).toArray()

    res.send({
        message: "data fetched",
        data: result
    })

})

// problem 11: who has registered the most recently ?
router.get('/problem-11', async (req, res) => {

    const result = await users.aggregate(
        [
            {
                $sort: {
                    registered: -1
                }
            }, {
                $limit: 1
            }
        ]
    ).toArray()

    res.send({
        message: "data fetched",
        data: result
    })

})

// problem 12: categorize "users" by their "favourite fruit" ?
router.get('/problem-12', async (req, res) => {

    const result = await users.aggregate(
        [
            {
                $group: {
                    _id: "$favoriteFruit",
                    users: {
                        $push: {
                            name: "$name",
                            _id: "$_id"
                        },
                    }
                }
            },
        ]
    ).toArray()

    res.send({
        message: "data fetched",
        data: result
    })

})

// problem13 : how many users have "ad" as a second tag in their list of tags ?
router.get('/problem-13', async (req, res) => {

    const result = await users.aggregate(
        [
            {
                $match: {
                    "tags.1": "ad"
                }
            },
            {
                $count: 'usersWithAdAsSecondTag'
            }
        ]
    ).toArray()

    res.send({
        message: "data fetched",
        data: result
    })

})

// problem 14: find users who have both "id" and "enim" as their tags ?
router.get('/problem-14', async (req, res) => {

    const result = await users.aggregate(
        [
            {
                $match: {
                    tags: {
                        $all: ["id", "enim"]
                    }
                }
            },
            {
                $project: {
                    name: 1,
                    _id: 1
                }
            }
        ]
    ).toArray()

    res.send({
        message: "data fetched",
        data: result
    })

})

// problem 15: list all companies located in "USA" with their coresponding user count ?
router.get('/problem-15', async (req, res) => {

    const result = await users.aggregate(
        [
            {
                $match: {
                    "company.location.country": "USA"
                }
            },
            {
                $group: {
                    _id: "$company.title",
                    users: {
                        $sum: 1
                    }
                }
            }
        ]
    ).toArray()

    res.send({
        message: "data fetched",
        data: result
    })

})

// aggregation in more than one collection by using $lookup

// problem 16: get author details from books collection from provided "author_id"
router.get('/problem-16', async (req, res) => {

    const result = await books.aggregate(
        [
            {
                $lookup: {
                    from: "authors",
                    localField: "author_id",
                    foreignField: "_id",
                    as: "author_details"
                }
            },
            {
                $addFields: {
                    author_details: {
                        $arrayElemAt: ["$author_details", 0]
                    }
                }
            }
        ]
    ).toArray()

    res.send({
        message: "data fetched",
        data: result
    })

})

export default router