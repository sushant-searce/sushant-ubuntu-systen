# SageMaker paths
prefix      = '/opt/ml/'
input_path  = os.path.join(prefix, 'input/data/')

# Adapted from https://github.com/pytorch/vision/blob/master/torchvision/datasets/mnist.py
class MyMNIST(data.Dataset):
    def __init__(self, train=True, transform=None, target_transform=None):
        self.transform = transform
        self.target_transform = target_transform
        self.train = train  # training set or test set
        # Loading local MNIST files in PyTorch format: training.pt and test.pt.
        if self.train:
            self.train_data, self.train_labels = 
                torch.load(os.path.join(input_path,'training/training.pt'))
        else:
            self.test_data, self.test_labels = 
                torch.load(os.path.join(input_path,'validation/test.pt'))

    def __getitem__(self, index):
        if self.train:
            img, target = self.train_data[index], self.train_labels[index]
        else:
            img, target = self.test_data[index], self.test_labels[index]
        # doing this so that it is consistent with all other datasets
        # to return a PIL Image
        img = Image.fromarray(img.numpy(), mode='L')
        if self.transform is not None:
            img = self.transform(img)
        if self.target_transform is not None:
            target = self.target_transform(target)
        return img, target

    def __len__(self):
        if self.train:
            return len(self.train_data)
        else:
            return len(self.test_data)
          
...

train_loader = torch.utils.data.DataLoader(
        MyMNIST(train=True,
                transform=transforms.Compose([
                           transforms.ToTensor(),
                           transforms.Normalize((0.1307,), (0.3081,))
                ])),
        batch_size=batch_size, shuffle=True, **kwargs)
    

test_loader = torch.utils.data.DataLoader(
        MyMNIST(train=False, 
                transform=transforms.Compose([
                           transforms.ToTensor(),
                           transforms.Normalize((0.1307,), (0.3081,))
                ])),
        batch_size=test_batch_size, shuffle=True, **kwargs)