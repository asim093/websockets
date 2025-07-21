import { MongoClient, ObjectId } from "mongodb";

const client = new MongoClient(process.env.MONGODB_CONNECTION_STRING);
let db;
const connectDB = async () => {
    if (!db) {
        await client.connect();
        db = client.db(process.env.DB_NAME);
    }
    return db;
};

export const getUserRole = async (userId) => {
    try {
        const database = await connectDB();
        const user = await database.collection('User').findOne({ _id: new ObjectId(userId) });
        return user ? user.role : null;
    } catch (error) {
        console.error("Error getting user role:", error);
        return null;
    }
};