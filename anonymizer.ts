import { faker } from '@faker-js/faker'
import { User } from './model'

export const anonymizeUser = (user: User) => {
    if (user.firstName) user.firstName = faker.string.alphanumeric(8)
    if (user.lastName) user.lastName = faker.string.alphanumeric(8)
    if (user.address?.line1) user.address.line1 = faker.string.alphanumeric(8)
    if (user.address?.line2) user.address.line2 = faker.string.alphanumeric(8)
    if (user.email) {
        const [, ...tail] = user.email.split('@')
        user.email = faker.string.alphanumeric(8) + tail.join('')
    }

    return user
}
