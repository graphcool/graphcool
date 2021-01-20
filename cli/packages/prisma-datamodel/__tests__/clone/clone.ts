import RelationalParser from '../../src/datamodel/parser/relationalParser'
import { cloneSchema } from '../../src/datamodel/model'

const AirBnBTestModel = `type User {
  id: ID! @unique
  createdAt: DateTime!
  updatedAt: DateTime!
  firstName: String!
  lastName: String!
  email: String! @unique
  password: String!
  phone: String!
  responseRate: Float
  responseTime: Int

  isSuperHost: Boolean! @default(value: "false")
  ownedPlaces: [Place!]!
  location: Location
  bookings: [Booking!]!
  paymentAccount: [PaymentAccount!]!
  sentMessages: [Message!]! @relation(name: "SentMessages")
  receivedMessages: [Message!]! @relation(name: "ReceivedMessages")
  notifications: [Notification!]!
  profilePicture: Picture
  hostingExperiences: [Experience!]!
}

type Place {
  id: ID! @unique
  name: String
  size: PLACE_SIZES
  shortDescription: String!
  description: String!
  slug: String!
  maxGuests: Int!
  numBedrooms: Int!
  numBeds: Int!
  numBaths: Int!
  reviews: [Review!]!
  amenities: Amenities!
  host: User!
  pricing: Pricing!
  location: Location!
  views: Views!
  guestRequirements: GuestRequirements
  policies: Policies
  houseRules: HouseRules
  bookings: [Booking!]!
  pictures: [Picture!]!
  popularity: Int!
}

type Pricing {
  id: ID! @unique
  place: Place!
  monthlyDiscount: Int
  weeklyDiscount: Int
  perNight: Int!
  smartPricing: Boolean! @default(value: "false")
  basePrice: Int!
  averageWeekly: Int!
  averageMonthly: Int!
  cleaningFee: Int
  securityDeposit: Int
  extraGuests: Int
  weekendPricing: Int
  currency: CURRENCY
}

type GuestRequirements {
  id: ID! @unique
  govIssuedId: Boolean! @default(value: "false")
  recommendationsFromOtherHosts: Boolean! @default(value: "false")
  guestTripInformation: Boolean! @default(value: "false")
  place: Place!
}

type Policies {
  id: ID! @unique
  createdAt: DateTime!
  updatedAt: DateTime!
  checkInStartTime: Float!
  checkInEndTime: Float!
  checkoutTime: Float!
  place: Place!
}

type HouseRules {
  id: ID! @unique
  createdAt: DateTime!
  updatedAt: DateTime!
  suitableForChildren: Boolean
  suitableForInfants: Boolean
  petsAllowed: Boolean
  smokingAllowed: Boolean
  partiesAndEventsAllowed: Boolean
  additionalRules: String
}

type Views {
  id: ID! @unique
  lastWeek: Int!
  place: Place!
}

type Location {
  id: ID! @unique
  lat: Float!
  lng: Float!
  neighbourHood: Neighbourhood
  user: User
  place: Place
  address: String
  directions: String
  experience: Experience
  restaurant: Restaurant
}

type Neighbourhood {
  id: ID! @unique
  locations: [Location!]!
  name: String!
  slug: String!
  homePreview: Picture
  city: City!
  featured: Boolean!
  popularity: Int!
}

type City {
  id: ID! @unique
  name: String!
  neighbourhoods: [Neighbourhood!]!
}

type Picture {
  url: String!
}

type Experience {
  id: ID! @unique
  category: ExperienceCategory
  title: String!
  host: User!
  location: Location!
  pricePerPerson: Int!
  reviews: [Review!]!
  preview: Picture!
  popularity: Int!
}

type ExperienceCategory {
  id: ID! @unique
  mainColor: String! @default(value: "#123456")
  name: String!
  experience: Experience
}

type Amenities {
  id: ID! @unique
  place: Place!
  elevator: Boolean! @default(value: "false")
  petsAllowed: Boolean! @default(value: "false")
  internet: Boolean! @default(value: "false")
  kitchen: Boolean! @default(value: "false")
  wirelessInternet: Boolean! @default(value: "false")
  familyKidFriendly: Boolean! @default(value: "false")
  freeParkingOnPremises: Boolean! @default(value: "false")
  hotTub: Boolean! @default(value: "false")
  pool: Boolean! @default(value: "false")
  smokingAllowed: Boolean! @default(value: "false")
  wheelchairAccessible: Boolean! @default(value: "false")
  breakfast: Boolean! @default(value: "false")
  cableTv: Boolean! @default(value: "false")
  suitableForEvents: Boolean! @default(value: "false")
  dryer: Boolean! @default(value: "false")
  washer: Boolean! @default(value: "false")
  indoorFireplace: Boolean! @default(value: "false")
  tv: Boolean! @default(value: "false")
  heating: Boolean! @default(value: "false")
  hangers: Boolean! @default(value: "false")
  iron: Boolean! @default(value: "false")
  hairDryer: Boolean! @default(value: "false")
  doorman: Boolean! @default(value: "false")
  paidParkingOffPremises: Boolean! @default(value: "false")
  freeParkingOnStreet: Boolean! @default(value: "false")
  gym: Boolean! @default(value: "false")
  airConditioning: Boolean! @default(value: "false")
  shampoo: Boolean! @default(value: "false")
  essentials: Boolean! @default(value: "false")
  laptopFriendlyWorkspace: Boolean! @default(value: "false")
  privateEntrance: Boolean! @default(value: "false")
  buzzerWirelessIntercom: Boolean! @default(value: "false")
  babyBath: Boolean! @default(value: "false")
  babyMonitor: Boolean! @default(value: "false")
  babysitterRecommendations: Boolean! @default(value: "false")
  bathtub: Boolean! @default(value: "false")
  changingTable: Boolean! @default(value: "false")
  childrensBooksAndToys: Boolean! @default(value: "false")
  childrensDinnerware: Boolean! @default(value: "false")
  crib: Boolean! @default(value: "false")
}

type Review @index(name: "RatingIndex", fields: ["stars", "checkIn", "value"], unique: false) {
  id: ID! @unique
  createdAt: DateTime!
  text: String!
  stars: Int!
  accuracy: Int!
  location: Int!
  checkIn: Int!
  value: Int!
  cleanliness: Int!
  communication: Int!
  place: Place!
  experience: Experience
}

type Booking {
  id: ID! @unique
  createdAt: DateTime!
  bookee: User!
  place: Place!
  startDate: DateTime!
  endDate: DateTime!
  payment: Payment!
}

type Payment {
  id: ID! @unique
  createdAt: DateTime!
  serviceFee: Float! @thisIsADirective(value: "database")
  placePrice: Float!
  totalPrice: Float!
  booking: Booking!
  paymentMethod: PaymentAccount!
}

type PaymentAccount {
  id: ID! @unique
  createdAt: DateTime!
  type: PAYMENT_PROVIDER
  user: User!
  payments: [Payment!]!
  paypal: PaypalInformation
  creditcard: CreditCardInformation
}

type PaypalInformation {
  id: ID! @unique
  createdAt: DateTime!
  email: String!
  paymentAccount: PaymentAccount!
}

type CreditCardInformation {
  id: ID! @unique
  createdAt: DateTime!
  cardNumber: String!
  expiresOnMonth: Int!
  expiresOnYear: Int!
  securityCode: String!
  firstName: String!
  lastName: String!
  postalCode: String!
  country: String!
  paymentAccount: PaymentAccount
}

type Message {
  id: ID! @unique
  createdAt: DateTime!
  from: User! @relation(name: "SentMessages")
  to: User! @relation(name: "ReceivedMessages")
  deliveredAt: DateTime!
  readAt: DateTime!
}

type Notification {
  id: ID! @unique
  createdAt: DateTime!
  type: NOTIFICATION_TYPE
  user: User!
  link: String!
  readDate: DateTime!
}

type Restaurant {
  id: ID! @unique
  createdAt: DateTime!
  title: String!
  avgPricePerPerson: Int!
  pictures: [Picture!]!
  location: Location!
  isCurated: Boolean! @default(value: "true")
  slug: String!
  popularity: Int!
}

enum CURRENCY {
  CAD
  CHF
  EUR
  JPY
  USD
  ZAR
}

enum PLACE_SIZES {
  ENTIRE_HOUSE
  ENTIRE_APARTMENT
  ENTIRE_EARTH_HOUSE
  ENTIRE_CABIN
  ENTIRE_VILLA
  ENTIRE_PLACE
  ENTIRE_BOAT
  PRIVATE_ROOM
}

enum PAYMENT_PROVIDER {
  PAYPAL
  CREDIT_CARD
}

enum NOTIFICATION_TYPE {
  OFFER
  INSTANT_BOOK
  RESPONSIVENESS
  NEW_AMENITIES
  HOUSE_RULES
}`

describe(`Schema clone functionality`, () => {
  test('Should clone a schema data structure correctly.', () => {
    const schema = new RelationalParser().parseFromSchemaString(AirBnBTestModel)

    let clone = cloneSchema(schema)

    // Deep equality check.
    expect(clone).toEqual(schema)

    // Now we check for inequality after a few mutations
    // This ensures we did not forget a reference somewhere
    clone = cloneSchema(schema)
    clone.types.filter(x => x.name === 'Restaurant')[0].isEnum = true
    expect(clone).not.toEqual(schema)

    clone = cloneSchema(schema)
    clone.types
      .filter(x => x.name === 'Message')[0]
      .fields.filter(x => x.name === 'from')[0].name = 'frum'
    expect(clone).not.toEqual(schema)

    clone = cloneSchema(schema)
    clone.types
      .filter(x => x.name === 'Amenities')[0]
      .fields.filter(x => x.name === 'babyMonitor')[0].defaultValue = 'true'
    expect(clone).not.toEqual(schema)

    clone = cloneSchema(schema)
    const directives = clone.types
      .filter(x => x.name === 'Payment')[0]
      .fields.filter(x => x.name === 'serviceFee')[0].directives
    expect(directives).not.toBeUndefined()
    if (directives !== undefined) {
      directives.filter(x => x.name === 'thisIsADirective')[0].arguments = {
        value: '"xmlFile"',
      }
    }
    expect(clone).not.toEqual(schema)
  })
})
