package com.limelight;

import android.accessibilityservice.AccessibilityService;
import android.accessibilityservice.AccessibilityServiceInfo;
import android.os.SystemClock;
import android.view.KeyEvent;
import android.view.accessibility.AccessibilityEvent;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

public class KeyInterceptorService extends AccessibilityService {

    public static volatile boolean isServiceRunning = false;
    public static volatile KeyInterceptorService instance = null;

    private void appendLine(String line) {
        try (FileOutputStream out = openFileOutput("keydiag.txt", MODE_APPEND);
             OutputStreamWriter writer = new OutputStreamWriter(out)) {
            writer.write(line);
            writer.write("\n");
            writer.flush();
        } catch (Exception ignored) {
        }
    }

    @Override
    protected void onServiceConnected() {
        super.onServiceConnected();
        isServiceRunning = true;
        instance = this;

        try {
            AccessibilityServiceInfo info = getServiceInfo();
            if (info != null) {
                info.flags |= AccessibilityServiceInfo.FLAG_REQUEST_FILTER_KEY_EVENTS;
                setServiceInfo(info);
            }
        } catch (Exception ignored) {
        }

        appendLine("=== SERVICE_CONNECTED " + System.currentTimeMillis() + " ===");
    }

    // Kept for compatibility with Game.java in this diagnostic build.
    // KeyDiag always keeps filtering requested while the service is enabled.
    public void updateKeyFiltering(boolean enable) {
        try {
            AccessibilityServiceInfo info = getServiceInfo();
            if (info != null) {
                info.flags |= AccessibilityServiceInfo.FLAG_REQUEST_FILTER_KEY_EVENTS;
                setServiceInfo(info);
            }
        } catch (Exception ignored) {
        }
    }

    @Override
    public void onDestroy() {
        appendLine("=== SERVICE_DESTROYED " + System.currentTimeMillis() + " ===");
        isServiceRunning = false;
        instance = null;
        super.onDestroy();
    }

    @Override
    public void onAccessibilityEvent(AccessibilityEvent event) {
    }

    @Override
    public void onInterrupt() {
        appendLine("=== SERVICE_INTERRUPTED " + System.currentTimeMillis() + " ===");
    }

    @Override
    protected boolean onKeyEvent(KeyEvent event) {
        String action;
        switch (event.getAction()) {
            case KeyEvent.ACTION_DOWN:
                action = "DOWN";
                break;
            case KeyEvent.ACTION_UP:
                action = "UP";
                break;
            case KeyEvent.ACTION_MULTIPLE:
                action = "MULTIPLE";
                break;
            default:
                action = Integer.toString(event.getAction());
                break;
        }

        String line =
                SystemClock.elapsedRealtimeNanos() +
                " action=" + action +
                " keyCode=" + event.getKeyCode() +
                "(" + KeyEvent.keyCodeToString(event.getKeyCode()) + ")" +
                " scanCode=" + event.getScanCode() +
                " metaState=0x" + Integer.toHexString(event.getMetaState()) +
                " deviceId=" + event.getDeviceId() +
                " source=0x" + Integer.toHexString(event.getSource()) +
                " flags=0x" + Integer.toHexString(event.getFlags()) +
                " repeat=" + event.getRepeatCount() +
                " ctrl=" + event.isCtrlPressed() +
                " alt=" + event.isAltPressed() +
                " shift=" + event.isShiftPressed() +
                " meta=" + event.isMetaPressed() +
                " fn=" + event.isFunctionPressed();

        appendLine(line);

        // Diagnostic only: never consume the event.
        return false;
    }
}
