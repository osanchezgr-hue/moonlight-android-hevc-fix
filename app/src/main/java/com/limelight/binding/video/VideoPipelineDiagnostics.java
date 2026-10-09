package com.limelight.binding.video;

import android.os.Handler;
import android.os.HandlerThread;
import android.os.Looper;
import android.os.SystemClock;
import android.view.Surface;

import com.limelight.LimeLog;
import com.limelight.nvstream.jni.MoonBridge;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Observational only: no IDR, codec recovery, sleeps or periodic polling.
 * Observer effects include atomics, uptime reads, output ownership objects,
 * a HandlerThread and MediaCodec callbacks per frame. No per-frame logging.
 */
public final class VideoPipelineDiagnostics {
    private static final long UNKNOWN = Long.MIN_VALUE;
    private static final AtomicLong SESSION_IDS = new AtomicLong();
    private final long sessionId = SESSION_IDS.incrementAndGet();
    private final AtomicLong epochIds = new AtomicLong();
    private final AtomicLong episodeIds = new AtomicLong();
    private final AtomicLong outputTokens = new AtomicLong();
    private final AtomicReference<Epoch> current = new AtomicReference<>();
    private final AtomicReference<Episode> episode = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<Handler> handler = new AtomicReference<>();
    private final AtomicReference<Runnable> pendingListener = new AtomicReference<>();
    private final AtomicReference<PostRequest> pendingPost = new AtomicReference<>();
    private final AtomicLong sessionFrames = new AtomicLong();
    private final AtomicLong sessionPictures = new AtomicLong();
    private final AtomicLong sessionConfigs = new AtomicLong();
    private final AtomicLong sessionOutputs = new AtomicLong();
    private final AtomicLong sessionRenders = new AtomicLong();
    private final AtomicLong sessionDiscards = new AtomicLong();
    private final AtomicLong sessionCallbacks = new AtomicLong();
    private final AtomicLong rejectedCallbacks = new AtomicLong();
    private final AtomicLong untrackedOutputIndices = new AtomicLong();
    private final AtomicLong unknownOwnership = new AtomicLong();
    private final AtomicLong sessionTryAgain = new AtomicLong();
    private final AtomicLong lastUniqueFrame = new AtomicLong(UNKNOWN);
    private final Beat submitEntry = new Beat(), submitExit = new Beat();
    private final Beat renderer = new Beat(), choreographer = new Beat();
    private final Beat inputWaitEntry = new Beat(), inputWaitExit = new Beat(), observer = new Beat();
    private final AtomicInteger submitInProgress = new AtomicInteger();
    private final AtomicInteger inputWaitInProgress = new AtomicInteger();
    private volatile Surface surface;
    private volatile String surfaceEvent = "none", codecOperation = "not-started", observerState = "starting";
    private volatile long surfaceEventUptimeMs;
    private volatile ErrorInfo error;
    private final HandlerThread observerThread;

    static final class Stage {
        final AtomicLong count = new AtomicLong();
        volatile long value = UNKNOWN, uptimeMs;
        void record(long value) {
            this.value = value;
            uptimeMs = SystemClock.uptimeMillis();
            count.incrementAndGet();
        }
        String text(String name, String valueName, long now) {
            return name + "{epochCount=" + count.get() + "," + valueName + "=" + number(value) +
                    ",ageMs=" + age(now, uptimeMs) + "}";
        }
    }

    static final class Epoch {
        final long id;
        // Diagnostic capacity only; never changes operational buffer/queue limits.
        final AtomicReferenceArray<Output> outputs = new AtomicReferenceArray<>(256);
        final Stage frames = new Stage(), pictures = new Stage(), configs = new Stage();
        final Stage decoded = new Stage(), rendered = new Stage(), callbacks = new Stage();
        final Stage discarded = new Stage();
        final AtomicLong lastUniqueFrame = new AtomicLong(UNKNOWN);
        volatile int frameType = -1;
        volatile long firstPicturePts = UNKNOWN, nativeReceiveMs, nativeEnqueueMs, renderTimeNanos;
        volatile long tryAgainStreak, maxTryAgainStreak;
        volatile String listener = "not-registered";
        Epoch(long id) { this.id = id; }
    }

    static final class Output {
        final Epoch epoch;
        final int index;
        final long token, ptsUs;
        Output(Epoch epoch, int index, long token, long ptsUs) {
            this.epoch = epoch; this.index = index; this.token = token; this.ptsUs = ptsUs;
        }
    }

    public static final class Episode {
        final long sessionId, id, preEpoch, preUptimeMs;
        volatile Integer pendingNativeFramesAtPre;
        volatile String nativePendingSource = "NOT_OBSERVED";
        Episode(long sessionId, long id, long preEpoch) {
            this.sessionId = sessionId; this.id = id; this.preEpoch = preEpoch;
            preUptimeMs = SystemClock.uptimeMillis();
        }
    }

    private static final class ErrorInfo {
        final String text;
        final long epoch, uptimeMs = SystemClock.uptimeMillis();
        ErrorInfo(String text, long epoch) { this.text = text; this.epoch = epoch; }
    }

    private static final class Beat {
        final AtomicLong count = new AtomicLong();
        volatile long uptimeMs;
        void record() { uptimeMs = SystemClock.uptimeMillis(); count.incrementAndGet(); }
        String text(String name, long now) {
            return name + "{count=" + count.get() + ",ageMs=" + age(now, uptimeMs) + "}";
        }
    }

    private static final class PostRequest {
        final Episode episode;
        final Runnable at500, at2000;
        PostRequest(Episode episode, Runnable at500, Runnable at2000) {
            this.episode = episode; this.at500 = at500; this.at2000 = at2000;
        }
    }

    VideoPipelineDiagnostics() {
        observerThread = new HandlerThread("Video - Pipeline diagnostics") {
            @Override protected void onLooperPrepared() {
                // No getLooper() wait on a video thread and no lifecycle monitor.
                final Handler ready;
                try { ready = new Handler(Looper.myLooper()); }
                catch (RuntimeException e) {
                    observerState = "handler-creation-failed";
                    error(e);
                    Looper looper = Looper.myLooper();
                    if (looper != null) looper.quitSafely();
                    return;
                }
                handler.set(ready);
                if (closed.get()) { Looper.myLooper().quitSafely(); return; }
                observerState = "ready";
                observer.record();
                Runnable install = pendingListener.getAndSet(null);
                if (install != null) ready.post(install);
                PostRequest posts = pendingPost.getAndSet(null);
                if (posts != null) enqueuePosts(ready, posts);
            }
        };
        try { observerThread.start(); }
        catch (RuntimeException e) { observerState = "start-failed"; error(e); }
    }

    Epoch epoch() { return current.get(); }
    boolean isCurrent(Epoch epoch) { return epoch != null && !closed.get() && current.get() == epoch; }
    void beginTransition(String operation) {
        current.set(null); // Retire all old stages and ownership; in-flight tokens retain their old epoch.
        codecOperation = operation;
    }
    Epoch activateEpoch(String operation) {
        if (closed.get()) return null;
        Epoch next = new Epoch(epochIds.incrementAndGet());
        codecOperation = operation;
        current.set(next);
        if (closed.get()) { current.compareAndSet(next, null); return null; }
        return next;
    }
    void codecOperation(String operation) { codecOperation = operation; }

    void completeFrame(Epoch epoch, int frame, int type, long receiveMs, long enqueueMs) {
        if (lastUniqueFrame.getAndSet(frame) != frame) sessionFrames.incrementAndGet();
        if (epoch != null && epoch.lastUniqueFrame.getAndSet(frame) != frame) {
            epoch.frameType = type; epoch.nativeReceiveMs = receiveMs; epoch.nativeEnqueueMs = enqueueMs;
            epoch.frames.record(frame);
        }
    }
    void inputAccepted(Epoch epoch, long ptsUs, boolean codecConfig) {
        (codecConfig ? sessionConfigs : sessionPictures).incrementAndGet();
        if (epoch != null) {
            if (!codecConfig && epoch.firstPicturePts == UNKNOWN) epoch.firstPicturePts = ptsUs;
            (codecConfig ? epoch.configs : epoch.pictures).record(ptsUs);
        }
    }
    Output outputDequeued(Epoch epoch, int index, long ptsUs) {
        sessionOutputs.incrementAndGet();
        boolean tracked = epoch != null && index >= 0 && index < epoch.outputs.length();
        if (index >= 256) untrackedOutputIndices.incrementAndGet();
        Output output = new Output(epoch, index, outputTokens.incrementAndGet(), tracked ? ptsUs : UNKNOWN);
        if (epoch != null) {
            epoch.decoded.record(ptsUs);
            if (tracked) epoch.outputs.set(index, output);
        }
        return output;
    }
    Output ownership(int index) {
        Epoch epoch = epoch();
        Output output = epoch != null && index >= 0 && index < epoch.outputs.length() ? epoch.outputs.get(index) : null;
        if (output == null) {
            unknownOwnership.incrementAndGet();
            // Unknown ownership contributes only to session totals, never epoch progress.
            output = new Output(null, index, 0, UNKNOWN);
        }
        return output;
    }
    void outputReleased(int index, Output output, boolean render) {
        (render ? sessionRenders : sessionDiscards).incrementAndGet();
        if (output == null || output.index != index || output.epoch == null) return;
        Epoch epoch = output.epoch;
        (render ? epoch.rendered : epoch.discarded).record(output.ptsUs);
        if (index >= 0 && index < epoch.outputs.length()) {
            // Identity CAS: do not erase a newer acquisition, even if index/PTS are identical.
            epoch.outputs.compareAndSet(index, output, null);
        }
    }
    void codecFrameRenderedCallback(Epoch epoch, long ptsUs, long renderTimeNanos) {
        observer.record();
        // PTS bounds also reject pre-flush notifications dispatched through a replaced listener.
        // A callback racing the acceptance observation is conservatively rejected, not called progress.
        if (!isCurrent(epoch) || epoch.firstPicturePts == UNKNOWN ||
                ptsUs < epoch.firstPicturePts || ptsUs > epoch.pictures.value) {
            rejectedCallbacks.incrementAndGet(); return;
        }
        sessionCallbacks.incrementAndGet();
        epoch.renderTimeNanos = renderTimeNanos;
        epoch.callbacks.record(ptsUs); // Writes only to the captured epoch, never a replacement epoch.
    }
    void mainDequeueResult(Epoch epoch, int result) {
        if (result == android.media.MediaCodec.INFO_TRY_AGAIN_LATER) sessionTryAgain.incrementAndGet();
        if (epoch != null) {
            // Single writer: the renderer's main 50ms dequeue. Zero-timeout drains never call this.
            epoch.tryAgainStreak = result == android.media.MediaCodec.INFO_TRY_AGAIN_LATER ? epoch.tryAgainStreak + 1 : 0;
            if (epoch.tryAgainStreak > epoch.maxTryAgainStreak) epoch.maxTryAgainStreak = epoch.tryAgainStreak;
        }
    }
    void submitEntered() { submitInProgress.incrementAndGet(); submitEntry.record(); }
    void submitExited() { submitExit.record(); submitInProgress.decrementAndGet(); }
    void inputWaitEntered() { inputWaitInProgress.incrementAndGet(); inputWaitEntry.record(); }
    void inputWaitExited() { inputWaitExit.record(); inputWaitInProgress.decrementAndGet(); }
    void rendererHeartbeat() { renderer.record(); }
    void choreographerHeartbeat() { choreographer.record(); }
    void observeSurface(Surface surface) { this.surface = surface; }
    void surfaceEvent(String event) { surfaceEvent = event; surfaceEventUptimeMs = SystemClock.uptimeMillis(); }
    void error(Throwable e) {
        Epoch epoch = epoch();
        String message = e.getClass().getSimpleName() + ":" + e.getMessage();
        error = new ErrorInfo(escape(message.substring(0, Math.min(message.length(), 160))), epoch == null ? -1 : epoch.id);
    }

    Handler observerHandler() { return closed.get() ? null : handler.get(); }
    void whenObserverReady(Runnable install) {
        if (closed.get()) return;
        Handler ready = observerHandler();
        if (ready != null) { ready.post(install); return; }
        pendingListener.set(install);
        ready = observerHandler();
        if (ready != null) {
            Runnable deferred = pendingListener.getAndSet(null);
            if (deferred != null) ready.post(deferred);
        }
    }
    Episode beginEpisode() {
        if (closed.get()) return null;
        Epoch epoch = epoch();
        Episode next = new Episode(sessionId, episodeIds.incrementAndGet(), epoch == null ? -1 : epoch.id);
        Episode previous = episode.getAndSet(next);
        if (closed.get()) { episode.compareAndSet(next, null); return null; }
        Handler ready = observerHandler();
        if (ready != null && previous != null) ready.removeCallbacksAndMessages(previous);
        return next;
    }
    boolean valid(Episode token) {
        return token != null && token.sessionId == sessionId && !closed.get() && episode.get() == token;
    }
    void postSnapshots(Episode token, Runnable at500, Runnable at2000) {
        if (!valid(token)) return;
        PostRequest posts = new PostRequest(token, at500, at2000);
        Handler ready = observerHandler();
        if (ready != null) { enqueuePosts(ready, posts); return; }
        pendingPost.set(posts);
        ready = observerHandler();
        if (ready != null) {
            PostRequest deferred = pendingPost.getAndSet(null);
            if (deferred != null) enqueuePosts(ready, deferred);
        }
    }
    private void enqueuePosts(Handler ready, PostRequest posts) {
        if (!valid(posts.episode)) return;
        ready.postAtTime(posts.at500, posts.episode, posts.episode.preUptimeMs + 500);
        ready.postAtTime(posts.at2000, posts.episode, posts.episode.preUptimeMs + 2000);
    }
    void observerHeartbeat() { observer.record(); }
    void close() {
        closed.set(true);
        observerState = "closed";
        episode.set(null); current.set(null);
        pendingListener.set(null); pendingPost.set(null);
        Handler ready = handler.get();
        if (ready != null) {
            ready.removeCallbacksAndMessages(null);
            ready.getLooper().quitSafely();
        }
        // If Looper is not ready, onLooperPrepared sees closed and quits on its own thread.
        // Never call HandlerThread.getLooper()/quitSafely() here: those can wait for startup.
    }

    private static long age(long now, long event) { return event == 0 ? -1 : Math.max(0, now - event); }
    private static String number(long value) { return value == UNKNOWN ? "unknown" : Long.toString(value); }
    private static String stage(Epoch epoch, Stage value, String name, String key, long now) {
        return epoch == null ? name + "{epochCount=0," + key + "=unknown,ageMs=-1}" : value.text(name, key, now);
    }
    private static String escape(String text) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < text.length(); i++) {
            if (out.length() >= 314) { out.append("..."); break; }
            char c = text.charAt(i);
            if (c == '\n') out.append("\\n");
            else if (c == '\r') out.append("\\r");
            else if (c == '\t') out.append("\\t");
            else if (c == '\\') out.append("\\\\");
            else if (Character.isISOControl(c) || c == '\u2028' || c == '\u2029') {
                out.append("\\u");
                String hex = Integer.toHexString(c);
                for (int pad = hex.length(); pad < 4; pad++) out.append('0');
                out.append(hex);
            } else out.append(c);
        }
        return out.toString();
    }

    void logSnapshot(Episode token, int snapshotId, String label, boolean stopping,
                     int recovery, int pacing, int queueSize) {
        if (!valid(token)) return;
        long now = SystemClock.uptimeMillis();
        Epoch epoch = epoch(); // One epoch object for all lines; samples within it are nontransactional.
        Surface observedSurface = surface;
        ErrorInfo observedError = error;
        String prefix = label + " sessionId=" + sessionId + " episodeId=" + token.id +
                " snapshotId=" + snapshotId + " codecEpochPre=" + token.preEpoch +
                " codecEpochObserved=" + (epoch == null ? -1 : epoch.id) +
                " snapshotUptimeMs=" + now + " elapsedFromPreMs=" + (now - token.preUptimeMs) + " nonTransactional=true";
        String input = " INPUT sessionFrames=" + sessionFrames.get() + " sessionPictures=" + sessionPictures.get() +
                " sessionConfigs=" + sessionConfigs.get() + " " + stage(epoch, epoch == null ? null : epoch.frames, "jniCompleteFrame", "frameNumber", now) +
                " frameType=" + (epoch == null ? -1 : epoch.frameType) + " frameTypeName=" +
                (epoch == null ? "unknown" : epoch.frameType == MoonBridge.FRAME_TYPE_IDR ? "IDR" :
                        epoch.frameType == MoonBridge.FRAME_TYPE_PFRAME ? "P" : "unknown") +
                " nativeReceiveMs=" + (epoch == null || epoch.frames.count.get() == 0 ? "unknown" : epoch.nativeReceiveMs) +
                " nativeEnqueueMs=" + (epoch == null || epoch.frames.count.get() == 0 ? "unknown" : epoch.nativeEnqueueMs) +
                " " + stage(epoch, epoch == null ? null : epoch.pictures, "acceptedPictureInputs", "codecPtsUs", now) +
                " " + stage(epoch, epoch == null ? null : epoch.configs, "acceptedCodecConfigInputs", "codecPtsUs", now) +
                " submitInProgress=" + submitInProgress.get() + " " + submitEntry.text("submitEntry", now) +
                " " + submitExit.text("submitExit", now) + " inputWaitInProgress=" + inputWaitInProgress.get() +
                " " + inputWaitEntry.text("inputWaitEntry", now) + " " + inputWaitExit.text("inputWaitExit", now);
        String output = " OUTPUT sessionOutputs=" + sessionOutputs.get() + " sessionRenders=" + sessionRenders.get() +
                " sessionDiscards=" + sessionDiscards.get() + " sessionCallbacks=" + sessionCallbacks.get() +
                " " + stage(epoch, epoch == null ? null : epoch.decoded, "decoderOutput", "codecPtsUs", now) +
                " " + stage(epoch, epoch == null ? null : epoch.rendered, "renderSubmitted", "codecPtsUs", now) +
                " " + stage(epoch, epoch == null ? null : epoch.discarded, "discardedOutput", "codecPtsUs", now) +
                " " + stage(epoch, epoch == null ? null : epoch.callbacks, "codecFrameRenderedCallback", "codecPtsUs", now) +
                " callbackRenderTimeNanos=" + (epoch == null || epoch.callbacks.count.get() == 0 ? "unknown" : epoch.renderTimeNanos) +
                " sessionMainTryAgain=" + sessionTryAgain.get() + " epochMainTryAgainStreak=" + (epoch == null ? 0 : epoch.tryAgainStreak) +
                " epochMaxMainTryAgainStreak=" + (epoch == null ? 0 : epoch.maxTryAgainStreak) +
                " rejectedCallbacks=" + rejectedCallbacks.get() + " untrackedOutputIndices=" + untrackedOutputIndices.get() +
                " unknownOwnership=" + unknownOwnership.get();
        String state = " STATE codecStartedObserved=" + (epoch != null) + " codecOperation=" + codecOperation +
                " stopping=" + stopping + " recoveryType=" + recovery + " listener=" + (epoch == null ? "inactive" : epoch.listener) +
                " surfacePresent=" + (observedSurface != null) + " surfaceValid=" +
                (observedSurface == null ? "unknown" : Boolean.toString(observedSurface.isValid())) +
                " surfaceEvent=" + surfaceEvent + " surfaceEventAgeMs=" + age(now, surfaceEventUptimeMs) +
                " framePacing=" + pacing + " outputQueueSize=" + queueSize +
                " pendingNativeFramesAtPre=" + (token.pendingNativeFramesAtPre == null ? "unavailable" : token.pendingNativeFramesAtPre) +
                " nativePendingSource=" + token.nativePendingSource +
                " " + renderer.text("rendererCycle", now) + " " + choreographer.text("choreographer", now) +
                " " + observer.text("diagnosticHandler", now) + " observerState=" + observerState + " lastError=" + (observedError == null ? "none" : observedError.text) +
                " errorEpoch=" + (observedError == null ? -1 : observedError.epoch) +
                " errorAgeMs=" + (observedError == null ? -1 : age(now, observedError.uptimeMs));
        // Cancellation is checked for each line; an already executing logger cannot be interrupted.
        if (valid(token)) LimeLog.info(prefix + input);
        if (valid(token)) LimeLog.info(prefix + output);
        if (valid(token)) LimeLog.info(prefix + state);
    }
}
