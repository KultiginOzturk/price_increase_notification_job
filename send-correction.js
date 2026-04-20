import { config } from 'dotenv';
import { existsSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';
import { MailerSend, EmailParams, Sender, Recipient } from 'mailersend';

const __dirname = dirname(fileURLToPath(import.meta.url));
const envPath = join(__dirname, '.env');
if (existsSync(envPath)) config({ path: envPath });

const FROM_EMAIL = 'cs@pestnotifications.com';
const FROM_NAME = 'Sherrill Pest Control';
const BCC_EMAIL = 'nate@pestanalytics.com';
const CORRECT_PRICE = '$30.75';
const INCORRECT_PRICE = '$123';

const RECIPIENTS = [
    { email: 'boyte.alanamarie@gmail.com', firstName: 'Alana',     fullName: 'Alana Boyte' },
    { email: 'alexlsmith14@yahoo.com',     firstName: 'Alexandra', fullName: 'Alexandra Smith' },
];

const TEMPLATE = `Hi {first_name},

We apologize for the multiple notifications you've received today. We are moving to a new email system, and clearly, we've had a few growing pains.

Please disregard the previous messages. To clarify, your monthly price is {correct_price}, not {incorrect_price}. This represents our first price adjustment since 2022.

We appreciate your patience as we upgrade our systems. Please reach out if you have any questions.

Best regards,
Sherrill Pest Control`;

const SUBJECT = 'Correction regarding your recent price-change notification';

const DRY_RUN = process.env.DRY_RUN === '1';

async function main() {
    const apiKey = process.env.MAILERSEND_API_KEY;
    if (!apiKey) {
        console.error('[send-correction] MAILERSEND_API_KEY missing');
        process.exit(1);
    }
    const mailerSend = new MailerSend({ apiKey });

    for (const r of RECIPIENTS) {
        const textContent = TEMPLATE
            .replace(/{first_name}/g, r.firstName)
            .replace(/{correct_price}/g, CORRECT_PRICE)
            .replace(/{incorrect_price}/g, INCORRECT_PRICE);

        console.log(`\n[send-correction] --> ${r.email} (${r.fullName})`);
        if (DRY_RUN) {
            console.log(textContent);
            continue;
        }

        const params = new EmailParams()
            .setFrom(new Sender(FROM_EMAIL, FROM_NAME))
            .setTo([new Recipient(r.email, r.fullName)])
            .setBcc([new Recipient(BCC_EMAIL)])
            .setSubject(SUBJECT)
            .setText(textContent);

        try {
            const response = await mailerSend.email.send(params);
            const messageId =
                response?.body?.['x-message-id'] ||
                response?.headers?.['x-message-id'] ||
                response?.messageId ||
                'unknown';
            console.log(`[send-correction] sent messageId=${messageId}`);
        } catch (err) {
            const body = err?.response?.body ?? err?.body;
            const msg = body?.message ?? (typeof body === 'string' ? body : JSON.stringify(body ?? err?.message ?? err));
            console.error(`[send-correction] FAILED for ${r.email}:`, msg);
        }
    }
}

main().catch((e) => {
    console.error('[send-correction] fatal:', e?.message || e);
    process.exit(1);
});
