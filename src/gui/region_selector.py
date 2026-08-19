# src/gui/region_selector.py
import logging

from PyQt6.QtCore import Qt, QPoint, QRect, QTimer
from PyQt6.QtGui import QColor, QPainter, QPen, QFont, QMouseEvent, QKeyEvent, QGuiApplication, QCursor
from PyQt6.QtWidgets import QDialog

from src.gui.input import InputLoop

logger = logging.getLogger(__name__)

# meikiocr's text detector has a fixed 960x544 input: regions much larger than that get
# heavily downscaled before detection, which can hurt accuracy on small manga/VN text.
# Google Lens has no such hard limit, but a tight region still means less to upload and
# less background noise for it to sift through, so the hint applies regardless of provider.
_LARGE_REGION_WIDTH = 1440
_LARGE_REGION_HEIGHT = 816

class RegionSelector(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setGeometry(self.get_current_screen(QCursor.pos()).geometry())

        # Window setup for a seamless overlay
        self.setWindowFlags(
            Qt.WindowType.FramelessWindowHint |
            Qt.WindowType.WindowStaysOnTopHint |
            Qt.WindowType.Tool
        )
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.setCursor(Qt.CursorShape.CrossCursor)

        # Points for drawing the overlay (in Qt's logical coordinates)
        self.begin_logical = QPoint()
        self.end_logical = QPoint()

        # Points for the final result (in physical coordinates)
        self.begin_physical = None
        self.end_physical = None
        self.selection_rect = None

        self.has_selection_started = False

        self.update_timer = QTimer(self)
        self.update_timer.setInterval(16)
        self.update_timer.timeout.connect(self.update_selection_rect)
        self.update_timer.start()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.fillRect(self.rect(), QColor(0, 0, 0, 100))

        if self.has_selection_started and not self.begin_logical.isNull() and not self.end_logical.isNull():
            rect_logical = QRect(self.begin_logical - self.geometry().topLeft(),
                                 self.end_logical - self.geometry().topLeft()).normalized()

            # Clear the selected area
            painter.setCompositionMode(QPainter.CompositionMode.CompositionMode_Clear)
            painter.fillRect(rect_logical, Qt.GlobalColor.transparent)
            painter.setCompositionMode(QPainter.CompositionMode.CompositionMode_SourceOver)

            # Draw the border, adjusted to be fully visible at the edges
            pen = QPen(QColor(30, 200, 255), 1, Qt.PenStyle.SolidLine)
            painter.setPen(pen)
            border_rect = rect_logical.adjusted(0, 0, -1, -1)
            painter.drawRect(border_rect)

            self._draw_size_readout(painter, rect_logical)

    def _draw_size_readout(self, painter: QPainter, rect_logical: QRect):
        if not self.begin_physical or not self.end_physical:
            return

        width = abs(self.end_physical.x() - self.begin_physical.x())
        height = abs(self.end_physical.y() - self.begin_physical.y())
        if width == 0 or height == 0:
            return

        text = f"{width} x {height} px"
        is_large = width > _LARGE_REGION_WIDTH or height > _LARGE_REGION_HEIGHT
        if is_large:
            text += "  (tip: a tighter region OCRs more accurately)"

        font = QFont()
        font.setPointSize(10)
        painter.setFont(font)
        painter.setPen(QPen(QColor(255, 210, 90) if is_large else QColor(230, 230, 230)))

        metrics = painter.fontMetrics()
        text_width = metrics.horizontalAdvance(text)
        label_x = max(0, min(rect_logical.left(), self.width() - text_width - 8))
        label_y = rect_logical.top() - 8
        if label_y < metrics.height():
            label_y = rect_logical.bottom() + metrics.height() + 4

        painter.fillRect(label_x - 4, label_y - metrics.ascent() - 2, text_width + 8,
                          metrics.height() + 4, QColor(0, 0, 0, 160))
        painter.drawText(label_x, label_y, text)

    def mousePressEvent(self, event: QMouseEvent):
        logger.debug("RegionSelector: mousePressEvent")
        self.begin_logical = QCursor.pos()
        if not self.begin_logical:  # when user selects upper left corner aka (0,0) aka None, the paint method won't work
            self.begin_logical = QPoint(1, 1)
        self.end_logical = self.begin_logical

        # Store the physical position for the final result
        px, py = InputLoop.get_mouse_pos()
        self.begin_physical = QPoint(px, py)
        self.end_physical = QPoint(px, py)

        self.has_selection_started = True
        self.update()

    def update_selection_rect(self):
        mouse_pos = QCursor.pos()
        if not self.has_selection_started:
            # Keep the window covering the screen where the mouse is
            current_screen = self.get_current_screen(mouse_pos)
            if current_screen and current_screen.geometry() != self.geometry():
                 self.setGeometry(current_screen.geometry())
            self.update()
            return

        self.end_logical = mouse_pos
        px, py = InputLoop.get_mouse_pos()
        self.end_physical = QPoint(px, py)
        self.update()

    def mouseReleaseEvent(self, event: QMouseEvent):
        logger.debug("RegionSelector: mouseReleaseEvent")
        self.update_timer.stop()

        # Get the final physical position
        px, py = InputLoop.get_mouse_pos()
        end_physical = QPoint(px, py)

        # Create the final selection rectangle using the stored physical coordinates
        self.selection_rect = QRect(self.begin_physical, end_physical).normalized()
        self.accept()

    def keyPressEvent(self, event: QKeyEvent):
        if event.key() == Qt.Key.Key_Escape:
            if self.update_timer.isActive():
                self.update_timer.stop()
            self.selection_rect = None
            self.reject()

    @staticmethod
    def get_current_screen(point):
        for screen in QGuiApplication.screens():
            if screen.geometry().contains(point):
                return screen
        return None

    @staticmethod
    def get_region():
        logger.info("Awaiting region selection... you can change the scan region in the tray")
        selector = RegionSelector()
        selector.show()
        selector.activateWindow()
        selector.raise_()
        if selector.exec() == QDialog.DialogCode.Accepted:
            return selector.selection_rect
        return None