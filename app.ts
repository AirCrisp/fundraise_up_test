import { connectToDB } from './db'
import { faker } from '@faker-js/faker'
import { User } from './model'

const createUsers = () => {
    const usersCount = faker.number.int({ max: 10, min: 1 })
    const users: User[] = []
    for (let i = 0; i < usersCount; i++) {
        users.push({
            firstName: faker.person.firstName(),
            lastName: faker.person.lastName(),
            email: faker.internet.email(),
            address: {
                line1: faker.location.streetAddress(),
                line2: faker.location.secondaryAddress(),
                postcode: faker.location.zipCode(),
                city: faker.location.city(),
                state: faker.location.state(),
                country: faker.location.country(),
            },
            createdAt: new Date(),
            updatedAt: new Date(),
        })
    }

    return users
};

(async () => {
    const client = await connectToDB()
    setInterval(async () => {
        const users = createUsers()
        client.db().collection('customers').insertMany(users)
    }, 200)
})()
