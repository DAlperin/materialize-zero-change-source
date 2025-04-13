import { Zero } from '@rocicorp/zero';
import { schema } from './schema';

const z = new Zero({
    userID: 'anon',
    server: 'http://localhost:4848',
    schema,
});

async function main() {
    let view = z.query.messages.where('author', 'dov').materialize();
    view.addListener(data => console.log('messages:', data));
    let view2 = z.query.current_messages.where('author', 'dov').materialize();
    view2.addListener(data => console.log('current_messages:', data));
    let view3 = z.query.expired_message_count.materialize();
    view3.addListener(data => console.log('expired_message_count:', data));

}

main().catch(console.error);
