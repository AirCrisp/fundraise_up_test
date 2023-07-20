import {
    Document,
    ChangeStreamInsertDocument,
    ChangeStreamUpdateDocument,
} from 'mongodb'
import {
    connectToDB,
    bulkUpsertCustomers,
    findMissedObjects,
    iterateCustomers,
    upsertEventToDoc,
} from './db'
import { DBRecordBuffer } from './buffer'
import { anonymizeUser } from './anonymizer'

const chunkSize = 1000
const timeoutMs = 1000

;(async () => {
    const client = await connectToDB()
    const db = client.db()
    const isReindexMode = process.argv[2] === '--full-reindex'

    const buff = new DBRecordBuffer(chunkSize, timeoutMs)
    console.log('Buffer created')

    buff.on('data', async (records) => {
        await bulkUpsertCustomers(records.map(anonymizeUser))
        buff.recordsChunkProcessed()
    })
    console.log('Data listener is set')

    if (isReindexMode) {
        console.log('Reindex mode started')
        buff.setTimer()
        const iterator = iterateCustomers(chunkSize)
        for await (const { records, i } of iterator) {
            console.log(
                `${(i - 1) * chunkSize + records.length} records processed`
            )
            buff.addRecords(records)
        }
        buff.on('records ended', () => process.exit(0))

        return
    }

    const changeStream = db.collection('customers').watch()
    changeStream.on(
        'change',
        (
            change:
                | ChangeStreamInsertDocument<Document>
                | ChangeStreamUpdateDocument<Document>
        ) => buff.addRecord(upsertEventToDoc(change))
    )
    console.log('Change listener is set')

    const lastAnonymized = await db
        .collection('customers_anonymised')
        .findOne({}, { sort: { updatedAt: 1 } })
    if (lastAnonymized) {
        const missedObjects = await findMissedObjects(
            'customers',
            lastAnonymized.updatedAt
        )
        if (missedObjects.length) {
            console.log('Missed changes found')
            buff.addRecords(missedObjects)
        }
    }
    buff.setTimer()
    console.log('Buffer timer started')
})()
