"""Templates the generator samples from and lightly mutates. Several
templates per category exist specifically so retrieval later has genuine
near-duplicates to find — a single hardcoded ticket per category would make
the downstream RAG step (see pipeline/stream_indexer.py) trivially easy to
look correct without actually exercising semantic similarity.
"""

PRODUCT_AREAS = ["billing", "auth", "performance", "integrations", "mobile-app"]

TEMPLATES = {
    "billing": [
        ("Charged twice for {plan} plan",
         "I was billed twice this month for my {plan} subscription. Invoice numbers are back to back, "
         "about {minutes} minutes apart. Can you refund the duplicate?"),
        ("Invoice shows wrong seat count",
         "Our invoice for {plan} lists {seats} seats but we only have {seats_actual} active users. "
         "Please correct the seat count and adjust the charge."),
        ("Can't update payment method",
         "The 'update card' button on the billing page just spins and never saves. Tried on {browser}."),
    ],
    "auth": [
        ("Locked out after password reset",
         "I reset my password from the email link but now login says 'invalid credentials' every time. "
         "Using {browser}, cleared cookies already."),
        ("SSO login redirects to blank page",
         "Since this morning, clicking 'Sign in with SSO' redirects to a blank white page instead of our IdP."),
        ("2FA codes not arriving",
         "Text message codes for two-factor login haven't arrived in over {minutes} minutes. Phone number is correct in settings."),
    ],
    "performance": [
        ("Dashboard takes over a minute to load",
         "The main dashboard has been taking {minutes}+ minutes to load since the last update. "
         "Other pages load normally."),
        ("Export job stuck at 0%",
         "Started a data export {minutes} minutes ago and the progress bar hasn't moved off 0%."),
        ("API responses much slower than usual",
         "Our integration's API calls that used to take ~200ms are now taking several seconds, started {minutes} minutes ago."),
    ],
    "integrations": [
        ("Slack notifications stopped working",
         "We used to get Slack alerts for new activity, they stopped about {minutes} minutes ago. Slack app still shows connected."),
        ("Webhook payloads missing fields",
         "The webhook payload used to include a 'customer_id' field, it's missing from recent deliveries."),
        ("Zapier integration returns 401",
         "Our Zap that used to work now fails with a 401 on every run, started this morning."),
    ],
    "mobile-app": [
        ("App crashes on launch (iOS)",
         "The app crashes immediately on open since updating to the latest version on iOS. Reinstalled, same result."),
        ("Push notifications delayed",
         "Push notifications are arriving {minutes}+ minutes late on Android."),
        ("Can't upload photos from camera roll",
         "Tapping 'upload photo' does nothing when selecting from camera roll, works fine for files."),
    ],
}

BROWSERS = ["Chrome", "Safari", "Firefox", "Edge"]
PLANS = ["Starter", "Growth", "Enterprise"]
