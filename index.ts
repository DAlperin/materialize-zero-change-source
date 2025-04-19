import "dotenv/config";
import Server from "./server";


const port = Number(process.env.PORT) || 8080;

const server = new Server();
server.startClientServer(port);