from PyQt5.QtWidgets import QApplication,QLineEdit,QPushButton,QCheckBox,QWidget, QVBoxLayout,QHBoxLayout,QLabel, QRadioButton,QGridLayout, QButtonGroup, QFileDialog
from PyQt5.QtCore import QTimer
import BLS_Request

proxies = {}
class secondWindow(QWidget):
    def __init__(self):
        super().__init__()
        layout = QVBoxLayout()
        self.setFixedSize(300,340)
        self.setWindowTitle("PROXY LOGIN")
        label = QLabel(self)
        label.setText("Proxy Username: ")
        newTextbox = QLineEdit(self)
        label1 = QLabel(self)
        label1.setText("Proxy Password: ")
        newTextbox1 = QLineEdit(self)
        layout.addWidget(label)
        layout.addWidget(newTextbox)
        layout.addWidget(label1)
        layout.addWidget(newTextbox1)
        submitButton = QPushButton("Save")
        submitButton.clicked.connect(lambda:proxySet(newTextbox.text(),newTextbox1.text(),self))
        layout.addWidget(submitButton)
        self.setLayout(layout)

def proxySet(Username,password,self):
    self.close()
    proxy_ip = "127.0.0.1"
    proxy_port = "5000"
    httpFull = "http://" + Username + ":" + password + "@" + proxy_ip + ":" + proxy_port
    BLS_Request.setProxy(httpFull)
def proxyLogin():
    app = QApplication([])
    w = secondWindow()
    w.show()
    app.exec_()