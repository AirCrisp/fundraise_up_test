import {
    MongoClient,
    Document,
    ChangeStreamInsertDocument,
    ChangeStreamUpdateDocument,
} from 'mongodb'
import { config } from 'dotenv'
config()
let mongoClient: MongoClient

export const connectToDB = async () => {
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

export const bulkUpsertCustomers = async (records: Document[]) => {
    const operations = records.map((record: Document) => ({
        updateOne: {
            filter: { _id: record._id },
            update: { $set: record },
            upsert: true,
        },
    }))

    await mongoClient.db().collection('customers').bulkWrite(operations)
}

export const findMissedObjects = async (
    collectionName: string,
    after: Date
) => {
    const missedObjects = await mongoClient
        .db('local')
        .collection('oplog.rs')
        .find({
            ns: new RegExp(`.${collectionName}$`),
            wall: { $gt: after, $lt: new Date() },
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

export async function* iterateCustomers(chunkSize: number) {
    let i = 0
    let records: Document[] = []
    while (i === 0 || records.length) {
        records = await mongoClient
            .db()
            .collection('customers')
            .find()
            .limit(chunkSize)
            .skip(i * chunkSize)
            .toArray()
        i++
        yield { records, i }
    }
}

export const upsertEventToDoc = (
    change:
        | ChangeStreamInsertDocument<Document>
        | ChangeStreamUpdateDocument<Document>
) => {
    if (change.operationType === 'insert') return change.fullDocument
    return {
        _id: change.documentKey._id,
        ...change.updateDescription?.updatedFields,
        updatedAt: new Date(),
    }
}
