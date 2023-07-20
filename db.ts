import { MongoClient, Collection, Document } from 'mongodb'
import { config } from 'dotenv'
config()

export const connectToDB = async () => {
    let mongoClient

    try {
        mongoClient = new MongoClient(process.env.DB_URI || '')
        console.log('Connecting to MongoDB...')
        await mongoClient.connect()
        console.log('Successfully connected to MongoDB!')

        return mongoClient
    } catch (error) {
        console.error('Connection to MongoDB failed!', error)
        process.exit()
    }
}

export const bulkUpsert = async (
    collection: Collection,
    records: Document[]
) => {
    const operations = records.map((record: Document) => ({
        updateOne: {
            filter: { _id: record._id },
            update: { $set: record },
            upsert: true,
        },
    }))

    await collection.bulkWrite(operations)
}

export const findMissedObjects = async (
    collectionName: string,
    before: Date,
    after: Date,
    client: MongoClient
) => {
    const missedObjects = await client
        .db('local')
        .collection('oplog.rs')
        .find({
            ns: new RegExp(`.${collectionName}$`),
            wall: { $gt: after, $lt: before },
            op: { $in: ['i', 'u'] },
        })
        .project({ _id: 0, o: 1, o2: 1 })
        .toArray()
    return missedObjects.map((obj: Document) => {
        if (obj.o2)
            return {
                ...obj.o2,
                ...obj.o.diff,
            }
        return obj.o
    })
}
