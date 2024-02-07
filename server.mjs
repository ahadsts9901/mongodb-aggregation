import express from 'express';
const app = express();
app.use(express.json()); // body parser

import mainRoutes from "./routes/main.mjs"
app.use("/api/v1", mainRoutes)

const PORT = process.env.PORT || 5002;
app.listen(PORT, () => {
    console.log(`Example server listening on port ${PORT}`)
})