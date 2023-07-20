import {
    Document,
    ChangeStreamInsertDocument,
    ChangeStreamUpdateDocument,
} from 'mongodb'
import { connectToDB, bulkUpsert, findMissedObjects } from './db'
import { DBRecordBuffer } from './buffer'
import { anonymizeUser } from './anonymizer'

const sourceCollection = 'customers'
const resultCollection = 'customers_anonymised'
const anonymize = anonymizeUser;

(async () => {
    const client = await connectToDB()
    const db = client.db()
    const isReindexMode = process.argv[2] === '--full-reindex'

    const buff = new DBRecordBuffer(1000, 1000)
    console.log('Buffer created')

    buff.on('data', async (records) => {
        await bulkUpsert(
            db.collection(resultCollection),
            records.map(anonymize)
        )
        buff.recordsChunkProcessed()
    })
    console.log('Data listener is set')

    const changeStream = db.collection(sourceCollection).watch()
    changeStream.on(
        'change',
        async (
            change:
                | ChangeStreamInsertDocument<Document>
                | ChangeStreamUpdateDocument<Document>
        ) => {
            if (change.operationType === 'insert')
                buff.addRecord(change.fullDocument)
            else
                buff.addRecord({
                    _id: change.documentKey._id,
                    ...change.updateDescription?.updatedFields,
                })
        }
    )
    console.log('Change listener is set')

    const lastAnonymized = await db
        .collection(resultCollection)
        .findOne({}, { sort: { createdAt: 1 } })
    if (lastAnonymized) {
        console.log('Last anonymized record found')
        const missedObjects = await findMissedObjects(
            sourceCollection,
            new Date(),
            lastAnonymized.createdAt,
            client
        )
        if (missedObjects.length) {
            console.log('Missed changes found')
            buff.addRecords(missedObjects)
        }
    }
    buff.setTimer()
    console.log('Buffer timer started')
})()
